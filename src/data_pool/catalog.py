import re
from pathlib import Path
import pandas as pd
import numpy as np
import yaml
import xarray as xr
from . import catalog_preprocessors as preprocessors

class DataCatalog:
    """
    DataCatalog class for discovering, listing, searching, and conditionally loading
    dataset files described by a YAML configuration.

    The class provides a lightweight registry over on-disk datasets where each
    dataset entry in the YAML config points to a directory (or glob pattern)
    containing one or more files. Files are inspected for version tokens (e.g.
    "v2", "V02.0") and returned to callers as pandas DataFrame rows or loaded as
    an :class:`xarray.Dataset` using :func:`xarray.open_mfdataset`.

    Parameters
    ----------
    yaml_config_path : str or pathlib.Path
        Path to a YAML configuration file describing available datasets. The YAML
        must contain a top-level "datasets" mapping. Each dataset key maps to an
        object with fields described in "YAML configuration" below.

    Attributes
    ----------
    config_path : pathlib.Path
        Resolved path to the provided YAML configuration file.
    datasets : dict
        The parsed "datasets" mapping from the YAML file. Each dataset entry is a
        dict that can include keys such as "path", "extension", "pattern",
        "display_name", "description", "tags", and "loader".
    
    YAML configuration
    ------------------
    Each dataset in the YAML should look like:
    datasets:
      my_dataset:
        path: /path/to/files
        extension: netcdf   # optional: used to build a glob if pattern not given
        pattern: '*.nc'     # optional: explicit glob pattern to use instead
        display_name: "My Dataset"   # optional user-facing name
        description: "Brief description"   # optional text used by search()
        tags:                     # optional list of searchable tags
          - ice
          - satellite
        loader: preprocess_func   # optional: name of preprocessor in preprocessors module
    
    Behavior and conventions
    ------------------------
    - File discovery:
      - If "pattern" is provided in the dataset config it is used as a glob
        (Path.glob). Otherwise a pattern is constructed from "extension" (if
        present) or matches all files.
      - Files are scanned for a version token using the regular expression
        r"(?:^|_|-)v(\d+(?:\.\d+)?)" (case-insensitive). Examples matched:
        "v2", "V02", "file_v2.0.nc".
      - Matched numeric versions are parsed to float. Files without a matched
        version use numpy.nan in the "version" column.
      - Discovered files are returned as a :class:`pandas.DataFrame` with columns
        "version" (float or NaN) and "path" (pathlib.Path).
    - Version semantics:
      - Methods that return "latest" prefer the highest numeric version (max on
        the "version" column) when any versioned files exist; otherwise they
        consider the first unversioned file.
      - Version lookup in .load(...) accepts numeric values (or strings coercible
        to float) and raises :class:`FileNotFoundError` when the requested version
        does not exist.

    Public methods
    --------------
    list_versions(dataset_key)
        Return a :class:`pandas.DataFrame` of discovered files for the given
        dataset_key. Raises :class:`KeyError` if the dataset_key is not present in
        the YAML config.
    latest_version(dataset_key)
        Return a tuple (version, [path]) where version is the numeric version
        (float) or :pydata:`numpy.nan` for unversioned files, and [path] is a
        single-element list containing the selected :class:`pathlib.Path`. Raises
        :class:`FileNotFoundError` when no files are found for the dataset.
    list_datasets()
        Return a :class:`pandas.DataFrame` summarizing all configured datasets.
        Columns include dataset_key, display_name, path, extension, versions,
        latest, and num_files.
    search(query)
        Perform a simple case-insensitive full-text search across "display_name",
        "description", and "tags" for each dataset. The query is split on
        whitespace and all words must be present in the combined text for a match.
        Returns a :class:`pandas.DataFrame` of matches.
    load(dataset_key, version=None, **xr_kwargs)
        Load dataset files for the given dataset_key using
        :func:`xarray.open_mfdataset`. If version is None, the latest numeric
        version is selected (if present); otherwise the supplied version is used.
        The loader/processor behavior:
          - If the dataset's YAML entry includes "loader", the value is treated as
            an attribute name on the imported `preprocessors` module and passed
            as the `preprocess` callable to xarray.open_mfdataset.
          - Files are passed as a list of string paths and opened with:
            combine='nested', concat_dim='time', chunks=None, engine='netcdf4'
          - Additional keyword arguments are forwarded to xarray.open_mfdataset.

        Returns
        -------
        xarray.Dataset
            The combined dataset returned by xarray.open_mfdataset.

    Exceptions (raises)
    -------------------
    - FileNotFoundError
        - If the YAML config path does not exist, or if no files/versions exist for
          a requested dataset or version.
    - KeyError
        - If a requested dataset_key is not present in the parsed YAML datasets.
    - ValueError
        - If the YAML config does not contain a top-level "datasets" mapping.

    Notes
    -----
    - Versions parsed from filenames are converted to float which means
      "v2.0" and "v2" are treated as the same numeric version (2.0).
    - The implementation expects a module named `preprocessors` to be importable
      if loader names are used in the YAML; the loader name should be the name of
      a function that accepts an xarray.Dataset and returns a transformed dataset.
    - Dependencies: :mod:`pathlib`, :mod:`yaml`, :mod:`pandas`, :mod:`numpy`,
      :mod:`re`, :mod:`xarray`.

    Examples
    --------
    Simple usage:
    >>> from pathlib import Path
    >>> cat = DataCatalog(Path("/etc/my_datasets.yaml"))
    >>> df = cat.list_versions("my_dataset")   # pandas.DataFrame with version/path
    >>> version, paths = cat.latest_version("my_dataset")
    >>> ds = cat.load("my_dataset", version=version)
    """
    
    # Initialize DataCatalog
    def __init__(self, yaml_config_path):
        self.config_path = Path(yaml_config_path)
        self.datasets = self._load_yaml(self.config_path)

    # -------------------------------
    # CONFIG LOADING
    # -------------------------------
    def _load_yaml(self, path):
        """
        Load and parse the YAML configuration file.
        """
        if not path.exists():
            raise FileNotFoundError(f"Config file not found: {path}")

        with open(path, "r") as f:
            cfg = yaml.safe_load(f)

        if "datasets" not in cfg:
            raise ValueError("YAML config must contain a 'datasets' section.")

        return cfg["datasets"]

    # -------------------------------
    # VERSION DISCOVERY
    # -------------------------------
    @staticmethod
    def _discover_versions(directory, extension="", pattern=None):
        """
        Discover files in a directory and extract version tokens from filenames.

        Parameters
        ----------
        directory : str or pathlib.Path
            Directory to search for dataset files.
        extension : str, optional
            File extension (without leading dot) used to build a glob pattern when
            ``pattern`` is not provided. Default is an empty string which matches
            any file.
        pattern : str, optional
            Explicit glob pattern to use (e.g. ``'*.nc'``). If provided, this
            pattern is used instead of constructing one from ``extension``.

        Returns
        -------
        pandas.DataFrame
            A DataFrame with two columns:
            - ``version`` (float or :pydata:`numpy.nan`): numeric version parsed
              from the filename using the regex ``r"(?:^|_|-)v(\d+(?:\.\d+)?)"`` (case-insensitive).
            - ``path`` (:class:`pathlib.Path`): path to the matched file.

        Notes
        -----
        - Files without a matching version token have ``version`` set to
          :pydata:`numpy.nan`.
        - Parsed versions are converted to ``float``, so ``v2`` and ``v2.0``
          are treated as the same numeric version.
        - If the directory does not exist or no files match the pattern, an
          empty DataFrame is returned.
        """
        # Prepare directory and file list
        directory = Path(directory)
        rows = []

        # Build glob pattern
        glob_pattern = pattern or f"*{('.' + extension.lstrip('.')) if extension else ''}"
        files = list(directory.glob(glob_pattern))

        # Regex: match v2, V02, V02.0, case-insensitive
        version_regex = re.compile(r"(?:^|_|-)v(\d+(?:\.\d+)?)", re.IGNORECASE)

        # Extract versions from filenames and append to rows
        for p in files:
            m = version_regex.search(p.name)
            if m:
                version = float(m.group(1))
            else:
                version = np.nan

            rows.append({
                "version": version,
                "path": p
            })

        # Create DataFrame from rows
        df = pd.DataFrame(rows)
        
        # Sort by version if not empty
        if not df.empty:
            df = df.sort_values("version").reset_index(drop=True)
        return df

    # -------------------------------
    # PUBLIC API
    # -------------------------------
    def list_versions(self, dataset_key):
        """
        List discovered files and versions for the given dataset key.

        Parameters
        ----------
        dataset_key : str
            The dataset key as defined in the YAML configuration.

        Returns
        -------
        pandas.DataFrame
            A DataFrame with two columns:

            - ``version`` (float or :pydata:`numpy.nan`): numeric version parsed
              from the filename using the regex ``r"(?:^|_|-)v(\d+(?:\.\d+)?)"``.
            - ``path`` (:class:`pathlib.Path`): path to the matched file.

            The DataFrame is sorted by the ``version`` column when non-empty.

        Raises
        ------
        KeyError
            If ``dataset_key`` is not present in the parsed YAML ``datasets``.
        FileNotFoundError
            If the configured dataset path does not exist or no files match the
            dataset's pattern (propagated from :meth:`_discover_versions` behavior).

        Notes
        -----
        This method delegates file discovery to :meth:`_discover_versions`, using
        the dataset's ``path``, optional ``extension``, and optional ``pattern``.
        """
        # Check dataset_key exists
        if dataset_key not in self.datasets:
            raise KeyError(f"Dataset '{dataset_key}' not found in config.")
        
        # Get dataset info
        info = self.datasets[dataset_key]
        directory = info["path"]
        extension = info.get("extension", "")
        pattern = info.get("pattern")

        # Discover and return versions
        return self._discover_versions(directory, extension, pattern)

    def latest_version(self, dataset_key):
        """
        Get the latest version and its path for a given dataset key.

        This routine queries the dataset versions (as returned by
        list_versions) and returns the most recent numeric version when
        versioned entries are present. If no numeric versions exist, it
        returns the first (unversioned) entry's version value and path.

        Parameters
        ----------
        dataset_key : str
            Identifier of the dataset for which to retrieve the latest version.

        Returns
        -------
        version : int or float or None
            The latest numeric version if available; otherwise the version
            value from the first unversioned entry (may be None or NaN).
        paths : list of str
            A single-element list containing the filesystem path corresponding
            to the selected latest entry.

        Raises
        ------
        FileNotFoundError
            If no versions are found for the provided ``dataset_key``.

        Notes
        -----
        This function expects :py:meth:`list_versions` to return a pandas.DataFrame
        with at least the columns ``version`` and ``path``. Numeric comparison is
        used to determine the latest version when version values are present.
        """

        # Get versions DataFrame
        df = self.list_versions(dataset_key)

        # Check for empty DataFrame
        if df.empty:
            raise FileNotFoundError(f"No versions found for dataset '{dataset_key}'.")
        
        # Return latest numeric version if available, else return the first unversioned entry
        df_versioned = df.dropna(subset=["version"])
        if not df_versioned.empty:
            latest_row = df_versioned.loc[df_versioned["version"].idxmax()]
        else:
            latest_row = df.iloc[0]
        
        # Return the version and path of the latest entry
        return latest_row["version"], [latest_row["path"]]

    def list_datasets(self):
        """
        List all available datasets with summary information.

        Returns
        -------
        pandas.DataFrame
            DataFrame with one row per dataset and the following columns:

            - ``dataset_key`` (str)
                Unique identifier for the dataset (the catalog key).
            - ``display_name`` (str)
                Human-readable name for the dataset; falls back to ``dataset_key`` when
                no display name is provided.
            - ``path`` (str)
                Filesystem or repository path where the dataset is stored.
            - ``extension`` (str)
                File extension associated with the dataset (empty string if unspecified).
            - ``versions`` (list)
                List of available version identifiers for the dataset (empty list if none).
            - ``latest``
                The latest version identifier (type depends on stored version values), or
                ``None`` if no versions exist.
            - ``num_files`` (int)
                Number of version entries/files recorded for the dataset.

        Notes
        -----
        This implementation calls :py:meth:`list_versions` for each dataset key to
        obtain per-dataset version information and derives ``versions``, ``latest``
        and ``num_files`` from that result.

        Exceptions
        ----------
        Any exceptions raised by :py:meth:`list_versions` (for example, due to I/O or
        malformed metadata) are propagated to the caller.
        """
        # Compile dataset summary rows
        rows = []
        for key, info in self.datasets.items():
            df = self.list_versions(key)
            rows.append({
                "dataset_key": key,
                "display_name": info.get("display_name", key),
                "path": info["path"],
                "extension": info.get("extension", ""),
                "versions": df["version"].tolist() if not df.empty else [],
                "latest": df["version"].max() if not df.empty else None,
                "num_files": len(df)
            })
        return pd.DataFrame(rows)

    # -------------------------------
    # SEARCH
    # -------------------------------
    def search(self, query):
        """
        Search the catalog for datasets matching a query string.

        Parameters
        ----------
        query : str
            Query string containing one or more whitespace-separated words. The
            query is lowercased and split on whitespace; each resulting word is
            treated as a case-insensitive substring to match. A dataset matches
            only if every word is present (logical AND) somewhere in the dataset's
            display name, description, or tags.

        Returns
        -------
        pandas.DataFrame
            A DataFrame with one row per matching dataset and the following columns:
            - dataset_key (str): the key identifying the dataset in the catalog.
            - display_name (str): human-readable name for the dataset (falls back to dataset_key).
            - path (str): filesystem or resource path for the dataset.
            - description (str): dataset description (empty string if not provided).
            - tags (list): list of tag strings associated with the dataset.

        Notes
        -----
        - Matching is performed by concatenating the dataset's display name,
          description, and tags, lowercasing this concatenated text, and checking
          that each query word appears as a substring.
        - Splitting is performed on whitespace; punctuation is not specially handled.
        - An empty or whitespace-only query will return all datasets (no filtering).
        - The order of rows in the returned DataFrame follows the iteration order of
          self.datasets (insertion order for standard Python dicts).

        Examples
        --------
        >>> # Return datasets that mention both "glacier" and "mass" anywhere in
        >>> # their display name, description, or tags (case-insensitive).
        >>> df = catalog.search("glacier mass")
        """

        # Split query into words and prepare results
        words = query.lower().split()
        rows = []

        # Search each dataset for all words
        for key, info in self.datasets.items():
            text = " ".join([
                info.get("display_name", key),
                info.get("description", ""),
                " ".join(info.get("tags", []))
            ]).lower()

            # Check if all words are present
            if all(word in text for word in words):
                rows.append({
                    "dataset_key": key,
                    "display_name": info.get("display_name", key),
                    "path": info["path"],
                    "description": info.get("description", ""),
                    "tags": info.get("tags", [])
                })

        # Return DataFrame of matches
        return pd.DataFrame(rows)

    # -------------------------------
    # CONDITIONAL DATA LOADING
    # -------------------------------
    def load(self, dataset_key, version=None, **xr_kwargs):
        """
        Load a dataset from the catalog into an xarray Dataset.

        This method queries available files for `dataset_key` via self.list_versions(...),
        selects an appropriate row based on the requested `version` (or the latest
        numeric version when `version` is None), determines an optional preprocessor
        based on the dataset metadata, and then opens the files with xarray.open_mfdataset.

        Parameters
        ----------
        dataset_key : str
            Key identifying the dataset in the catalog. This is used to look up available
            files (with versions and paths) via self.list_versions(dataset_key) and to
            find loader/preprocessor metadata in self.datasets.
        version : int, float or str, optional
            Desired dataset version. If None (default) the function will:
              - prefer the largest numeric ("version") entry if any versioned rows exist,
              - otherwise select the first unversioned row.
            If a specific version is provided, it is compared numerically (the code
            converts the provided value to float). If no matching version is found a
            FileNotFoundError is raised.
        **xr_kwargs : dict, optional
            Additional keyword arguments forwarded to xarray.open_mfdataset.

        Returns
        -------
        xarray.Dataset
            Combined dataset produced by xarray.open_mfdataset. The call uses
            combine='nested', concat_dim='time', preprocess=<callable or None>,
            chunks=None and engine='netcdf4' by default; any supplied **xr_kwargs are
            forwarded and may override or extend these options.

        Raises
        ------
        FileNotFoundError
            If no files exist for `dataset_key`, or if a specific `version` was requested
            but not found.

        Notes
        -----
        - The DataFrame returned by self.list_versions(dataset_key) is expected to
          contain at least the columns 'version' and 'path'. 'version' may be NaN for
          unversioned entries.
        - A preprocessor callable is determined by looking up the dataset's "loader"
          value in self.datasets and resolving that name in the preprocessors module.
          If no loader is configured, no preprocessing function is passed to xarray.
        - All file paths present in the DataFrame's 'path' column are converted to
          strings and passed to xarray.open_mfdataset. Any errors raised by xarray
          (e.g. I/O or decoding errors) are propagated to the caller.

        Examples
        --------
        >>> # Load the latest numeric version for 'my_dataset'
        >>> ds = catalog.load('my_dataset')
        >>> # Load a specific version (numeric or numeric-like string)
        >>> ds_v2 = catalog.load('my_dataset', version=2)
        >>> # Pass additional xarray options
        >>> ds = catalog.load('my_dataset', chunks={'time': 100}, decode_times=False)
        """
        
        # Get versions DataFrame
        df = self.list_versions(dataset_key)
        if df.empty:
            raise FileNotFoundError(f"No files exist for dataset '{dataset_key}'.")

        # Split versioned and unversioned datasets
        df_versioned = df.dropna(subset=["version"])
        df_unversioned = df[df["version"].isna()]

        # Determine row(s) to load
        if version is None:
            # Select latest version
            if not df_versioned.empty:
                row = df_versioned.loc[df_versioned["version"].idxmax()]
            # Select first unversioned
            elif not df_unversioned.empty:
                row = df_unversioned.iloc[0]
            else:
                raise FileNotFoundError(f"No files found for dataset '{dataset_key}'.")
        else:
            # Select specific version
            row = df_versioned.loc[df_versioned["version"] == float(version)]
            if row.empty:
                raise FileNotFoundError(f"Version {version} not found for dataset '{dataset_key}'.")
            row = row.iloc[0]

        # Get list of paths to load
        paths = [str(p) for p in df['path']]

        # If a loader is specified, get the preprocessing function
        loader_name = self.datasets[dataset_key].get("loader")
        preprocess_func = getattr(preprocessors, loader_name) if loader_name else None

        # Load dataset with xarray
        ## NOTE: open_mfdataset uses dask under the hood for lazy loading and handles single and mult-file cases
        return xr.open_mfdataset(
            paths,
            combine='nested',
            concat_dim='time',
            preprocess=preprocess_func,
            chunks=None,
            engine='netcdf4',
            **xr_kwargs
        )

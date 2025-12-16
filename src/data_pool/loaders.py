from pathlib import Path
import pandas as pd
import xarray as xr
import geopandas as gpd 
import numpy as np
import rioxarray as rxr
import os
from datetime import datetime
import re

def default(self, row, **kwargs):

    # Extract parameters from row
    path, ext, skip_lines, no_data, ignore_dirs, ignore_files = self._extract_row_params(row)
        
    # Find files
    files = self._recursive_find_files(path, ext, ignore_dirs = ignore_dirs, ignore_files = ignore_files)

    # If no files found, raise error
    if not files:
        raise FileNotFoundError(
            f"No files found with extension '{ext}' in {path}\n"
            f"Ignoring directories: {ignore_dirs}"
        )

    # Load based on extension type
    # ------------------------------
    # CSV -> Pandas
    if ext == "csv":

        # Get kwargs (or set defaults)
        low_memory = kwargs.pop("low_memory", False)
        
        # Load each CSV into a DataFrame and append to data_list
        data_list = []
        for f in files:
            data = pd.read_csv(f, skiprows = skip_lines, low_memory = low_memory, **kwargs)
            data_list.append(data)
        
        # Concatenate all CSVs into a single DataFrame
        output = pd.concat(data_list, ignore_index = True)

        # Replace no_data values with NaN if specified
        if no_data is not None:
            output = output.replace(no_data, np.nan)

        return output 

    # GPKG / SHP -> GeoPandas
    if ext in ("gpkg", "shp"):
        
        # Load each file into a GeoDataFrame and append to data_list
        data_list = []
        for f in files:
            data = gpd.read_file(f)
            data_list.append(data)
        
        # Concatenate all GeoDataFrames into a single GeoDataFrame
        output = gpd.GeoDataFrame(pd.concat(data_list, ignore_index = True))

        # Replace no_data values with NaN if specified
        if no_data is not None:
            output = output.replace(no_data, np.nan)

        return output

    # TIF -> rioxarray / xarray
    if ext in ("tif"):

        # Get kwargs (or set defaults)
        masked = kwargs.pop("masked", True)
        
        # Load each TIF into an xarray DataArray and store in a dict. Using masked = True automates handling of NaN values.
        data_dict = {}
        for f in files:
            name = f.stem
            data = rxr.open_rasterio(f, masked = masked)

            # Squeeze out single-size 'band' dimension if present
            if "band" in data.dims and data.band.size == 1:
                data = data.squeeze("band", drop = True)

            # Store in dict
            data_dict[name] = data

        # Combine all DataArrays into a single Dataset
        output = xr.Dataset(data_dict)

        return output

    # NetCDF -> xarray
    if ext in ("nc"):

        # Get kwargs (or set defaults)
        combine = kwargs.pop("combine", "by_coords")

        output = xr.open_mfdataset(files,
                                    combine = combine,
                                    **kwargs)

        return output

    raise ValueError(f"Extension '{ext}' is currently not supported for loading. Use one of: csv, gpkg, shp, tif, nc.")


def _extract_year_range_from_filename(filename):
    """
    Extract start and end years from a filename.
    
    Assumes years are represented as four-digit numbers (e.g., 1990, 2020) in the filename.
    For example, from a filename like:
        - Antarctica_ice_velocity_<start_year>_<end_year>_1km_<version>.nc
        - Central_Antarctica_ice_velocity_<year>.nc
    it will extract the years and return them as integers.

    Parameters
    ----------
        filename (str): The filename to extract years from.

    Returns
    -------
        tuple: A tuple containing the start year and end year as integers.
    """

    # Use regex to find all four-digit year patterns in the filename
    base_name = os.path.basename(filename)
    years = re.findall(r'(19\d{2}|20\d{2})', base_name)
    years = [int(y) for y in years]

    # If no years found, raise an error
    if not years:
        raise ValueError(f"No years found in filename: {filename}")

    # Return the first and last years found
    years = sorted(set(years))
    return years[0], years[-1]

def measures_velocity(self, row, **kwargs):
    
    def _preprocessor(ds):
        """
        Preprocess each dataset to add a time dimension based on the year(s) in the filename.

        This function extracts the year from the filename, calculates the middle year,
        creates a time dimension set to July 2nd of that year, and expands the dataset
        to include this time dimension.

        Parameters
        ---------
            ds (xarray.Dataset): The input dataset to preprocess.

        Returns
        ---------
            xarray.Dataset: The preprocessed dataset with an added time dimension.

        Notes
        ---------
            - The time is set to July 2nd of the middle year (or the single year if only one is present).
            - Assumes the filename is stored in ds.encoding['source'].
        """
        
        # Extract year(s) from filename
        start_year, end_year = _extract_year_range_from_filename(ds.encoding['source'])
        mid_year = start_year + (end_year - start_year) // 2
        
        # Create time dimension (July 2nd of the extracted year)
        time_da = xr.DataArray([datetime(year = mid_year, month = 7,  day = 2)], dims = 'time')

        # Expand dataset to include time dimension
        return ds.expand_dims(time = time_da)

    # Extract parameters from row
    path, ext, skip_lines, no_data, ignore_dirs, ignore_files = self._extract_row_params(row)
        
    # Find files
    files = self._recursive_find_files(path, ext, ignore_dirs = ignore_dirs, ignore_files = ignore_files)

    # If no files found, raise error
    if not files:
        raise FileNotFoundError(
            f"No files found with extension '{ext}' in {path}\n"
            f"Ignoring directories: {ignore_dirs}"
        )

    # Get kwargs (or set defaults)
    combine = kwargs.pop("combine", "by_coords")

    # Load NetCDF files with preprocessing to add time dimension. Combine by (all) coordinates.
    output = xr.open_mfdataset(files,
                            combine = combine,
                            preprocess = _preprocessor,
                            parallel = True,
                            **kwargs)
    
    return output
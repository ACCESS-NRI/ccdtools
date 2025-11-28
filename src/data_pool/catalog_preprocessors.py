from datetime import datetime
import os
import xarray as xr

def prepro_annual_time(ds):
    """
    Add a time dimension to annual files without one.

    Parameters
    ----------
    ds : xarray.Dataset
        Dataset to preprocess. Must have a 'source' entry in
        ``ds.encoding`` pointing to a filename that contains a 4-digit year.

    Returns
    -------
    xarray.Dataset
        The input dataset expanded with a time coordinate set to
        January 1st of the year extracted from the filename.

    Raises
    ------
    ValueError
        If 'source' is missing from ``ds.encoding`` or no 4-digit year is found
        in the filename.
    """
    
    # Get source filename
    source = ds.encoding.get('source')
    if source is None:
        raise ValueError("Dataset has no 'source' in encoding for preprocessing.")

    # Look for a 4-digit year in the filename
    fname = os.path.basename(source)
    year = None
    for part in fname.split('_'):
        if part.isdigit() and len(part) == 4:
            year = int(part)
            break
    if year is None:
        raise ValueError(f"No 4-digit year found in filename: {fname}")

    # Create time coordinate and expand dataset
    time_da = xr.DataArray([datetime(year = year, month = 1, day = 1)], dims = 'time')
    ds = ds.expand_dims(time = time_da)

    return ds
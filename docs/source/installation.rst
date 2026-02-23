Installation
============

.. warning::
   This is an experimental release. Documentation remains a work in progress. Some sections may be incomplete or under development.

`access-cryosphere-data-pool` can be installed using `pip` or `conda`, or by cloning the repository directly from GitHub. Below are instructions for each method.

Installing via PyPI
------------------
You can install `access-cryosphere-data-pool` using PyPI, the Python package manager.
The latest CCD release can be found on PyPI here: https://pypi.org/project/access-cryosphere-data-pool/

To install CCD using PyPI, run the following command in your terminal:

.. code-block:: bash

   pip install access-cryosphere-data-pool

Installing via conda
--------------------
The `access-cryosphere-data-pool` can be installed using conda via the `accessnri` channel:  
https://anaconda.org/channels/accessnri/packages/access-cryosphere-data-pool/overview

.. code-block:: bash

   conda install accessnri::access-cryosphere-data-pool

Installing from GitHub (development version)
--------------------------------------------
You can also install the latest development version directly from the GitHub repository:

1. Clone the repository:

   .. code-block:: bash

      git clone https://github.com/ACCESS-NRI/access-cryosphere-data-pool.git

2. Navigate to the cloned directory:

   .. code-block:: bash

      cd access-cryosphere-data-pool

3. Install `access-cryosphere-data-pool` using pip:

   .. code-block:: bash

      pip install .

.. note::
   Installing from GitHub is recommended only if you want the latest features or are contributing to `access-cryosphere-data-pool` development.

Verifying the Installation
--------------------------
After installation, you can verify the desired `access-cryosphere-data-pool` version is installed correctly by running the following commands in a Python shell:

.. code-block:: python

   import datapool
   print(datapool.__version__)
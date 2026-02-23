Installation
============

.. warning::
   This is an experimental release. Documentation remains a work in progress. Some sections may be incomplete or under development.

The Cryosphere Community Datapool (CCD) can be installed using PyPI or conda, or by cloning the repository directly from GitHub. Below are instructions for each method.

Installing via PyPI
------------------
You can install CCD using PyPI, the Python package manager.
The latest CCD release can be found on PyPI here: https://pypi.org/project/access-cryosphere-data-pool/

To install CCD using PyPI, run the following command in your terminal:

.. code-block:: bash

   pip install access-cryosphere-data-pool

Installing via conda
--------------------
CCD can be installed using conda via the `accessnri` channel:  
https://anaconda.org/channels/accessnri/packages/access-cryosphere-data-pool/overview

.. code-block:: bash

   conda install accessnri::access-cryosphere-data-pool

Installing from GitHub (development version)
--------------------------------------------
You can also install the latest development version of CCD directly from the GitHub repository:

1. Clone the repository:

   .. code-block:: bash

      git clone https://github.com/ACCESS-NRI/access-cryosphere-data-pool.git

2. Navigate to the cloned directory:

   .. code-block:: bash

      cd access-cryosphere-data-pool

3. Install local clone of CCD using `pip`:

   .. code-block:: bash

      pip install .

.. note::
   Installing from GitHub is recommended only if you want the latest features or are contributing to CCD development.

Using and verifying the installation
--------------------------
After installation is complete, CCD should be available for use in your chosen Python environment.

To use CCD, import the library into a workflow and check the installed version, run the following:

.. code-block:: python

   import ccd
   print(ccd.__version__)
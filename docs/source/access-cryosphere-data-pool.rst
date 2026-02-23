.. _api:

ACCESS Cryosphere Community Datapool API Reference
==================================================
.. automodule:: ccd
.. currentmodule:: ccd

.. _data-catalog:

DataCatalog
-----------
The ACCESS Cryosphere Community Datapool is developed around a cenral ``DataCatalog`` class.

.. autosummary::
    :toctree: api

    catalog.DataCatalog

.. _data-discovery:

Data discovery
--------------
There are various methods to explore the DataCatalog:

.. autosummary::
   :toctree: api
   :recursive:

    catalog.DataCatalog.available_resolutions
    catalog.DataCatalog.available_subdatasets
    catalog.DataCatalog.available_versions
    catalog.DataCatalog.help
    catalog.DataCatalog.search

.. _loading_data:

Loading data
------------
Datasets are loaded using the ``load_dataset`` method.

.. autosummary::
   :toctree: api
   :recursive:

   catalog.DataCatalog.load_dataset

Under-the-hood, datasets are loaded using custom ``loaders``. 

.. autosummary::
   :toctree: api
   :recursive:

   loaders
|Show Logo|
=================
MongoDB vs Pandas
=================

Companion project to our article on Medium: `MongoDB vs Pandas`_.  We are using `zKillboard API`_ as a data source for demoing data transformations.  

Getting Started
===============

    ``pip install -r requirements.txt``
    
    ``python pandas_zkb_test.py``

Project is designed to run on any Python 3.5+ system.  Try out our methods on your own machine!

Expected Results
================

**System Config**

- Windows 10x64
- Python 3.5.1
- i7-4769K @ 4.00GHz
- 32GB DDR3 @ 1333MHz

+-------------------------------+-------------+--------------+---------------+
| Data Scenario                 | 200 records | 2000 records | 50000 records |
+===============================+=============+==============+===============+
| Pivot Dicts - Pandas          | 0.015s      | 0.041s       | 0.804s        |
+-------------------------------+-------------+--------------+---------------+
| Pivot Dicts - Raw Python      | 0.028s      | 0.217s       | 5.781s        |
+-------------------------------+-------------+--------------+---------------+
| Pivot Lists - Pandas (concat) | 0.834s      | 9.672s       | 2069.694s     |
+-------------------------------+-------------+--------------+---------------+
| Pivot Lists - Raw Python      | 0.043s      | 0.213s       | 5.502s        |
+-------------------------------+-------------+--------------+---------------+

.. _MongoDB vs Pandas:
.. _zKillboard API: 
.. |Show Logo| image:: http://dl.eveprosper.com/podcast/logo-colour-17_sm2.png

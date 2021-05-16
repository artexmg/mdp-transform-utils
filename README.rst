========
Overview
========

.. start-badges

.. list-table::
    :stub-columns: 1

    * - docs
      - |docs|
    * - tests
      - |
        |
        | |codeclimate|
    * - package
      - | |commits-since|
.. |docs| image:: https://readthedocs.org/projects/mdp-transform-utils/badge/?style=flat
    :target: https://readthedocs.org/projects/mdp-transform-utils
    :alt: Documentation Status

.. |codeclimate| image:: https://codeclimate.com/github/marathonoil/mdp-transform-utils/badges/gpa.svg
   :target: https://codeclimate.com/github/marathonoil/mdp-transform-utils
   :alt: CodeClimate Quality Status

.. |commits-since| image:: https://img.shields.io/github/commits-since/marathonoil/mdp-transform-utils/v0.0.0.svg
    :alt: Commits since latest release
    :target: https://github.com/marathonoil/mdp-transform-utils/compare/v0.0.0...master



.. end-badges

Transformation Utils for Upstram FSM2

Installation
============

::

    pip install transform-utils

You can also install the in-development version with::

    pip install https://github.com/marathonoil/mdp-transform-utils/archive/master.zip


Documentation
=============


https://mdp-transform-utils.readthedocs.io/


Development
===========

To run all the tests run::

    tox

Note, to combine the coverage data from all the tox environments run:

.. list-table::
    :widths: 10 90
    :stub-columns: 1

    - - Windows
      - ::

            set PYTEST_ADDOPTS=--cov-append
            tox

    - - Other
      - ::

            PYTEST_ADDOPTS=--cov-append tox

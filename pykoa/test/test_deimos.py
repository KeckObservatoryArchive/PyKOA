import pytest
import os
import pykoa
from pykoa.koa import Koa 
from astropy.table import Table,Column


OUTPUT_PATH = './output'

def test_query_by_date_range():
    pass
if __name__ == '__main__':
    if not os.path.exists(OUTPUT_PATH):
       os.mkdir(OUTPUT_PATH)


    Koa.query_datetime ('deimos', \
    '2015-09-01 00:00:00/2015-09-30 23:59:59', \
    './output/DEIMOS2015.vot', overwrite=True, format='votable', \
    server='http://vmkoatest.ipac.caltech.edu/' )

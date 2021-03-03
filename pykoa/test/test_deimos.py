
import pdb
from pykoa.koa import Koa 
import pytest
import os
from astropy.table import Table,Column

OUTPUT_PATH = './testOutput'
if not os.path.exists(OUTPUT_PATH):
   os.mkdir(OUTPUT_PATH)
FILE_TYPES = {'ipac': '.tbl', 'tsv': '.tsv', 'votable': '.vot', 'csv':'.csv'}
INSTR = 'deimos'

outFileName = os.path.join(OUTPUT_PATH, INSTR.upper())
SERVER = 'https://koa.ipac.caltech.edu/'  # must have end backslash

# queries
daterange = '2015-09-01 00:00:00/2015-09-30 23:59:59'
circle = 'circle 268.288 65.097 1.0'
obj = 'ngc_1614' 
box = "select koaid from koa_deimos where \
           (contains(point('j2000',ra/dec ), box('j2000', 23.48 ,30.60, 1, 1))=1) "
polygon = "select koaid, filehand, ra, dec from koa_deimos\
        where contains(point('icrs', ra/dec), \
        polygon('icrs',209.80225,53.34894,209.80225,55.34894,211.80225,54.34894)) = 1"

cone = "select koaid, object, koaimtyp, frameno, ra, dec,obstype, \
            to_char(date_obs,'yyyy-mm-dd'),ut as date_obs, ha, az, detector, gratenam, \
            dwfilval,focusmod, focusval, gratesta,tempdet, rotatmod, ccdgain, \
            pane1, pane2, pane3, pane4, pane5, ttime, \
            progid, proginst,  progpi, progtitl, semester, outfile,filehand \
            from koa_deimos where \
            (contains(point('j2000',ra/dec ), circle('j2000', 23.48 ,30.60, 1, 1))=1) \
            order by utdatetime" 
instdatepos = {
        'instrument':'deimos',
        'datetime':'2018-12-03 00:00:00/2018-12-04 23:59:59',
        'pos':'circle 23.48 30.60 1.0'
        }
boxcolsort = "select top 10 koaid, ra2000 ,dec2000, utdatetime from koa_hires \
           where (contains(point('j2000',ra/dec),  \
           box('j2000', 23.48 ,30.60, 1, 1)) =1) order by utdatetime desc "
boxcount = "select count(koaid) from koa_deimos \
    where (contains(point('j2000', ra/dec), box('j2000', 23.48 ,30.60, 1, 1 ))=1) "
programinfo = "select koaid, filehand, progid from koa_deimos where (progid = 'C117') " 

# helper functions
query_adql = lambda qy, fn, fm: Koa.query_adql(qy, fn, overwrite=True, format=fm, server=SERVER)
get_nth_pair = lambda d, n: [x for x in iter(d.items())][-1]
query_criteria = lambda prm, fn, fm: Koa.query_criteria(prm, fn, overwrite=True, format=fm, server=SERVER)
def adqlize(instr, fun):
    '''makes fun more like query_adql'''
    def nugget(qy, fn, fm):
        return fun(instr, qy, fn, overwrite=True, format=fm, server=SERVER)
    return nugget

def read_table(fileName, fileType):
    if 'tsv' in fileType:  # Table.read format alias
        fileType = 'ascii.fast_tab'
    return Table.read(fileName, format=fileType)

test_query_params = [
        ('programinfo', query_adql, programinfo),
        ('polygon', query_adql, polygon),
        ('boxcolsort', query_adql, boxcolsort),
        ('cone', query_adql, cone),
        ('datetime', adqlize(INSTR, Koa.query_datetime), daterange), 
        ('circle', adqlize(INSTR, Koa.query_position), circle), 
        ('object', adqlize(INSTR, Koa.query_object), obj), 
        ('box', query_adql, box), 
        ('boxcount', query_adql, boxcount),
        ('criteria', query_criteria, instdatepos),
        ]
test_ids = [x[0] for x in test_query_params]

# tests
@pytest.mark.parametrize("fileType,extension", list(FILE_TYPES.items()), ids = list(FILE_TYPES.keys()))
def test_query_by_date_range(fileType, extension):
    fileName = outFileName + extension
    if os.path.exists(fileName):
        os.remove(fileName)
    Koa.query_datetime (INSTR, daterange, 
                        fileName, overwrite=True, format=fileType,
                        server=SERVER)
    assert os.path.exists(fileName), f'file {fileName} did not download'
    assert read_table(fileName, fileType), f'file {fileName} unable to be read'

@pytest.mark.parametrize("queryName,fun,query", test_query_params, ids=test_ids)
def test_query(queryName, fun, query):
    fileType, extension = get_nth_pair(FILE_TYPES, 0)
    fileName = outFileName +'_' + queryName + extension
    fun(query, fileName, fileType)
    assert os.path.exists(fileName), f'file {fileName} did not download'
    assert read_table(fileName, fileType), f'file {fileName} unable to be read'


    

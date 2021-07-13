import os
import sys
import io
import filecmp
import pytest

from pykoa.koa import Koa 
from astropy.table import Table,Column


#userdict = {
#   "taptestadm_maunakea":"Successfully login as taptestadm",
#   "xxtaptestadm_maunakea":"Failed to login: userid [xxtaptestadm] is invalid.",
#   "taptestadm_xxmaunakea":"Failed to login: Incorrect password [xxmaunakea] for userid [taptestadm]"
#}

#
#    test login method: correctly, wrong userid, and wrong password
#
#@pytest.mark.parametrize ("user, expected", list(userdict.items()), ids=list(userdict.keys()))  
 
#def test_login_success (user, expected, capsys):
    
#    ind = user.find('_')
#    userid = user[0:ind]
#    password = user[ind+1:]
    
#    Koa.login ('./taptestadmcookie.txt', \
#        userid=userid, \
#        password=password)
        
#    out, err = capsys.readouterr()
#    assert out.startswith (expected)


#
#   Test query_datetime method for all instruments; returns ipac format tables. 
#   Pass if the returned IPAC table is identical to the truth data.
#
#fmtdict = {
#    "ipac":".tbl",
#    "votable":".xml",
#    "csv":".csv",
#    "tsv":".tsv"
#}



#
#    test query_datetime method of all instruments
#
#datetimedict = {
#    "hires":"2019-10-24 00:00:00/2019-10-24 23:59:59",
#    "deimos":"2019-10-24 00:00:00/2019-10-24 23:59:59",
#    "osiris":"2019-10-23 00:00:00/2019-10-23 23:59:59",
#    "esi":"2019-09-01 00:00:00/2019-09-01 23:59:59"
#}

#@pytest.mark.parametrize ("instr,datetime", list(datetimedict.items()), \
#    ids=list(datetimedict.keys()))
 
#def test_query_datetime (instr, datetime, capsys):

#    outpath = './datetime_' + instr + '.tbl'
#    datapath = './truthdata/datetime_' + instr + '.tbl'
 
#    Koa.query_datetime (instr, \
#        datetime, \
#        outpath, \
#        cookiepath='./taptestadmcookie.txt')
        
#    assert os.path.exists(outpath), \
#        f'Result not downloaded to file [{outpath:s}]'
#    assert (filecmp.cmp (outpath, datapath, shallow=False))


#
#    test query_position method of all instruments
#
#    hires_0: 91 recs,
#    hires_1: 91 recs,
#    hires_2: 28 recs,
#    deimos : 7 recs,
#    osiris : 198 recs,
#    esi    : 13 recs,
#

#positiondict = {
#    "hires_0":"circle 77.28 -8.748 0.5",
#    "hires_1":"box 77.28 -8.748 1.0 1.0",
#    "hires_2":"polygon 209.80225 53.34894 209.80225 55.34894 211.80225 55.34894 211.80 53.34",
#    "deimos":"circle 68.50 -8.58 0.5",
#    "osiris":"circle 66.77 26.10 0.58",
#    "esi":"circle 35.44 -5.54 0.5"
#}

#@pytest.mark.parametrize ("instrument,pos", list(positiondict.items()), \
#    ids=list(positiondict.keys()))
 
#def test_query_position (instrument, pos, capsys):

#    print (f'instrument={instrument:s}')
#    print (f'pos={pos:s}')
    
#    ind = instrument.find ('_')
#    if (ind != -1):
#        instr = instrument[0:ind]
#    else:
#        instr = instrument
#    print (f'instr={instr:s}')
    
#    ind = pos.find (' ')
#    shape = pos[0:ind]
#    print (f'shape={shape:s}')

#    outpath = './pos_' + shape + '_' + instr + '.tbl'
#    datapath = './truthdata/pos_' + shape + '_' + instr + '.tbl'

#    Koa.query_position (instr, \
#        pos, \
#        outpath, \
#        cookiepath='./taptestadmcookie.txt')
        
#    assert os.path.exists(outpath), \
#        f'Result not downloaded to file [{outpath:s}]'
#    print (f'here0')

#    astropytbl = None
#    astropytbl = Table.read (outpath, format='ascii.ipac')
#    assert (astropytbl is not None), \
#        "f{outpath:s} cannot be read by astropy"
#    print (f'here1')

#    astropytbl_truth = None
#    astropytbl_truth = Table.read (datapath, format='ascii.ipac')
#    assert (astropytbl_truth  is not None), \
#        "f{datapath:s} cannot be read by astropy"
#    print (f'here1-1')

#    assert (len(astropytbl) >= len(astropytbl_truth)), \
#        f"Number of records in {outpath:s} is incorrect"
#    print (f'here2: len(astropytbl) = {len(astropytbl):d}')


#
#    test query_object method of all instruments
#
#    hires : hr1679 -- 69 recs,
#    deimos: NGC 1614  -- 7 recs,
#    osiris: DG_Tau -- 198 recs,
#    esi   : NGC895 -- 14 recs 
#
#objectdict = {
#    "hires" : "hr1679",
#    "deimos": "NGC 1614",
#    "osiris": "DG_Tau",
#    "esi"   : "NGC895"
#}

#@pytest.mark.parametrize ("instr,object", list(objectdict.items()), \
#    ids=list(objectdict.keys()))
 
#def test_query_object (instr, object, capsys):

#    outpath = './object_' + instr + '.tbl'
#    datapath = './truthdata/object_' + instr + '.tbl'
 
#    Koa.query_object (instr, \
#        object, \
#        outpath, \
#        cookiepath='./taptestadmcookie.txt')
        
#    assert os.path.exists(outpath), \
#        f'Result not downloaded to file [{outpath:s}]'
    
#    astropytbl = None
#    astropytbl = Table.read (outpath, format='ascii.ipac')
#    assert (astropytbl is not None), \
#        "f{outpath:s} cannot be read by astropy"
#    print (f'here1')

#    astropytbl_truth = None
#    astropytbl_truth = Table.read (datapath, format='ascii.ipac')
#    assert (astropytbl_truth  is not None), \
#        "f{datapath:s} cannot be read by astropy"
#    print (f'here1-1')

#    assert (len(astropytbl) >= len(astropytbl_truth)), \
#        f"Number of records in {outpath:s} is incorrect"
#    print (f'here2: len(astropytbl) = {len(astropytbl):d}')

    
    


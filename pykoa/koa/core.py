"""
Copyright (c) 2020, California Institute of Technology (Caltech). 

This software was developed by the Keck Observatory Archive (KOA), a
collaboration between the NASA Exoplanet Science Institute (NExScI) and
the W. M. Keck Observatory (WMKO). NExScI is sponsored by NASA's
Exoplanet Exploration Program, and operated by the
California Institute of Technology in coordination with the Jet
Propulsion Laboratory (JPL).

All rights not granted herein are expressly reserved by Caltech.

Redistribution and use in source and binary forms for academic and other 
non-commercial purposes, with or without modification, are permitted 
provided that the following conditions are met:

    Redistributions of source code, including modified source code, must 
    retain the above copyright notice, this list of conditions and the 
    following disclaimer.

    Redistributions in binary form or a modified form of the source code 
    must reproduce the above copyright notice, this list of conditions and 
    the following disclaimer in the documentation and/or other materials 
    provided with the distribution.

    Neither the name of the California Institute of Technology, the names 
    of its employees, nor the names of its contributors may be used to 
    endorse or promote products derived from this software without specific
    prior written permission.

    Where a modified version of the source code is redistributed publicly 
    in source or binary forms, the modified source code must be published 
    in a freely accessible manner, or otherwise redistributed at no charge
    to anyone requesting a copy of the modified source code, subject to the
    same terms as this agreement.


THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" 
AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE 
IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE 
ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE 
LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR 
CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF 
SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS 
INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN 
CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) 
ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE 
POSSIBILITY OF SUCH DAMAGE.
"""

import os 
import sys
import io
import getpass 
import logging
import time
import json
import lxml
#import ijson
import xmltodict 
import tempfile
import bs4 as bs

import requests
import urllib 
import http.cookiejar

from datetime import date
#from astropy.coordinates import name_resolve
from astropy.table import Table, Column

from . import conf

class Archive:
#
#{ Archive class
#
    """
    The 'Archive' class provides functions for accessing data stored in the 
    Keck Observatory Archive (KOA). Queries are performed via the nexsciTAP
    server.
    
    Keck PIs can use the KOA credentials assigned to them when data were 
    acquired (given at login) to search for their proprietary data.

    Example:
    --------

    import os
    import sys 

    from pykoa.koa import Koa 

    Koa.query_datetime ('hires', \
        '2018-03-16 00:00:00/2018-03-18 00:00:00', \
        outpath= './meta.xml', \
        format='ipac') 
    """
    
    tap = None

    parampath = ''
    outpath = ''
    format = 'ipac'
    maxrec = -1 
    query = ''
    propflag = 1

    content_type = ''
    outdir = ''
    astropytbl = None

    ndnloaded = 0
    ndnloaded_lev1 = 0
    nlev1list = 0
    ndnloaded_calib = 0
    ncaliblist = 0
 
    status = ''
    msg = ''
    
    debugfname = './koa.debug'    
    debug = 0    

    def __init__(self, **kwargs):
#
#{ Archive.init
#
        """
        'init' method initializes the class with optional debugfile flag.

        Optional inputs:
        ----------------
        debugfile: a file path for the debug output
 
	"""
        
        if ('debugfile' in kwargs):
            
            self.debug = 1
            self.debugfname = kwargs.get ('debugfile')

            if (len(self.debugfname) > 0):
      
                logging.basicConfig (filename=self.debugfname, \
                    level=logging.DEBUG)
    
                with open (self.debugfname, 'w') as fdebug:
                    pass
 
        if self.debug:
            logging.debug ('')
            logging.debug ('Enter koa.init:')

#
#    retrieve baseurl from conf class;
#
#    during dev or test, baseurl will be a keyword input
#
        if self.debug:
            logging.debug ('')
            logging.debug (f'conf.server= {conf.server:s}')

        self.baseurl = conf.server
        if ('server' in kwargs):
            self.baseurl = kwargs.get ('server')
        
        len_baseurl = len (self.baseurl)
        if (self.baseurl[len_baseurl-1] != '/'):
            self.baseurl + '/'

        if self.debug:
            logging.debug ('')
            logging.debug (f'baseurl= {self.baseurl:s}')
            logging.debug ('')
            logging.debug (f'conf.cgipgm= {conf.cgipgm:s}')

        self.cgipgm = conf.cgipgm
        if ('cgipgm' in kwargs):
            self.cgipgm = kwargs.get ('cgipgm')
        
        if self.debug:
            logging.debug ('')
            logging.debug (f'cgipgm= {self.cgipgm:s}')

#
#    urls for nph-tap.py, nph-koaLogin, nph-makeQyery, 
#    nph-getKoa, and nph-getCaliblist
#
        self.tap_url = self.baseurl + self.cgipgm
        
        self.login_url = self.baseurl + 'cgi-bin/KoaAPI/nph-koaLogin?'
        self.makequery_url = self.baseurl + 'cgi-bin/KoaAPI/nph-makeQuery?'
        self.caliblist_url = self.baseurl+ 'cgi-bin/KoaAPI/nph-getCaliblist?'
        self.lev1list_url = self.baseurl + 'cgi-bin/KoaAPI/nph-getL1list?'
        self.getkoa_url = self.baseurl + 'cgi-bin/getKOA/nph-getKOA?return_mode=json&'

        if self.debug:
            logging.debug ('')
            logging.debug (f'login_url= [{self.login_url:s}]')
            logging.debug (f'tap_url= [{self.tap_url:s}]')
            logging.debug (f'makequery_url= [{self.makequery_url:s}]')
            logging.debug (f'self.getkoa_url= {self.getkoa_url:s}')
            logging.debug (f'self.caliblist_url= {self.caliblist_url:s}')
      
        return
#
#} end Archive.init
#


    def login (self, cookiepath, **kwargs):
#
#{ Archive.login
#
        """
        'login' method validates a user has a valid KOA account; it takes two
        'keyword' arguments: userid and password. If the inputs are not 
        provided in the keyword, the login method prompts for inputs.

        Required input:
        ---------------     
        cookiepath (string): a file path provided by the user to save 
                 returned cookie (in login method) or to serve
                 as input parameter for the subsequent koa 
                 query and download methods.
        
        Keyword input:
        ---------------     
	userid     (string): a valid user id assigned by KOA;
        
        password   (string): a valid password in the KOA's user table; 

        
        Calling synopsis: 
    
        koa.login (cookiepath, userid='xxxx', password='xxxxxx'), or

        koa.login (cookiepath): and the program will prompt for 
                                 userid and password 
        """

        if (self.debug == 0):

            if ('debugfile' in kwargs):
            
                self.debug = 1
                self.debugfname = kwargs.get ('debugfile')

                if (len(self.debugfname) > 0):
      
                    logging.basicConfig (filename=self.debugfname, \
                        level=logging.DEBUG)
    
                    with open (self.debugfname, 'w') as fdebug:
                        pass

            if self.debug:
                logging.debug ('')
                logging.debug ('debug turned on')
        
#
#    if server keyword represent during dev/test, modify baseurl
#
        if self.debug:
            logging.debug ('')
            logging.debug (f'conf.server= {conf.server:s}')

        self.baseurl = conf.server

        if self.debug:
            logging.debug ('')
            logging.debug (f'baseurl (from conf)= {self.baseurl:s}')
        
        if ('server' in kwargs):
            self.baseurl = kwargs.get ('server')
        
        if self.debug:
            logging.debug ('')
            logging.debug (f'baseurl= {self.baseurl:s}')
        

        
        if self.debug:
            logging.debug ('')
            logging.debug ('')
            logging.debug ('Enter login:')
            logging.debug (f'cookiepath= [{cookiepath:s}]')

        if (len(cookiepath) == 0):
            print ('A cookiepath is required if you wish to login to KOA')
            return

        userid= ''
        password = ''
        if ('userid' in kwargs):
            userid = kwargs.get ('userid')

        if ('password' in kwargs):
            password = kwargs.get ('password')

        url = ''
        response = ''
        jsondata = ''

        status = ''
        msg = ''

#
#    get userid and password via keyboard input
#
        if (len(userid) == 0):
            userid = input ("Userid: ")

        if (len(password) == 0):
            password = getpass.getpass ("Password: ")

        password = urllib.parse.quote (password)


        self.login_url = self.baseurl + 'cgi-bin/KoaAPI/nph-koaLogin?'
        
        if self.debug:
            logging.debug ('')
            logging.debug (f'login_url= [{self.login_url:s}]')

        param = dict()
        param['userid'] = userid
        param['password'] = password
    
        data_encoded = urllib.parse.urlencode (param)
    
        url = self.login_url + data_encoded

        if self.debug:
            logging.debug ('')
            logging.debug (f'url= [{url:s}]')


#
#     cookiejar declared and linked to cookiepath
#
        if self.debug:
            logging.debug ('')
            logging.debug ('declare request session with cookie')
        
        session = requests.Session()
        session.cookies = http.cookiejar.MozillaCookieJar (cookiepath)
        cookiejar = session.cookies

        response = None
        try:
            response = session.get (url, cookies=cookiejar)
        
        except Exception as e:

            msg = 'Failed to login: ' + str(e)
            print (msg)
            return

        if self.debug:
            logging.debug ('')
            logging.debug ('response.text: ')
            logging.debug (response.text)
            logging.debug ('response.headers: ')
            logging.debug (response.headers)
       
#
#    check content-type in response header: 
#    it should be an 'application/json' structure, 
#    parse for return status and message
#
        contenttype = response.headers['Content-type']
        
        if self.debug:
            logging.debug ('')
            logging.debug (f'contenttype= {contenttype:s}')

        jsondata = json.loads (response.text);
   
        for key,val in jsondata.items():
                
            if (key == 'status'):
                status = val
                
            if (key == 'msg'):
                msg =  val
		
        if self.debug:
            logging.debug ('')
            logging.debug (f'status= {status:s}')
            logging.debug (f'msg= {msg:s}')


        if (status == 'ok'):
#            cookiejar.save (cookiepath, ignore_discard=True);
            
            cookiejar.save ()
        
            msg = 'Successfully login as ' + userid
            self.cookie_loaded = 1

#
#    print out cookie values in debug file
#   
            for cookie in cookiejar:
                    
                if self.debug:
                    logging.debug ('')
                    logging.debug ('cookie saved:')
                    logging.debug (cookie)
                    logging.debug (f'cookie.name= {cookie.name:s}')
                    logging.debug (f'cookie.value= {cookie.value:s}')
                    logging.debug (f'cookie.domain= {cookie.domain:s}')
 
        else:       
            msg = 'Failed to login: ' + msg

        print (msg)
        return
#
#} end Archive.login
#


    def query_datetime (self, instrument, datetime, outpath, **kwargs):
#
#{ Archive.query_datetime
#
        
        """
        'query_datetime' method searches KOA by 'datetime' range
        
        Required Inputs:
        ---------------    
        instrument (string): HIRES

        datetime (string): a datetime string in the format of 
            datetime1/datetime2 where '/' separates the two datetime values
            of format 'yyyy-mm-dd hh:mm:ss'

            the following inputs are acceptable:

            datetime1/: will search data with datetime later than (>=) 
                        datetime1
            
            /datetime2: will search data with datetime earlier than (<=)
                        datetime2

            datetime1: will search data with datetime equal to (=) datetime1

        outpath (string): a full output filepath of the returned metadata 
            table
    
        e.g. 
            instrument = 'hires',
            datetime = '2018-03-16 06:10:55/2018-03-18 00:00:00' 

        e.g. 
            instrument = 'hires',
            datetime = '2018-03-16 06:10:55/' 

        e.g. 
            instrument = 'hires',
            datetime = '/2018-03-18 00:00:00' 

        e.g. 
            instrument = 'hires',
            datetime = '2018-03-16 06:10:55' 

        Optional inputs:
	----------------
        cookiepath (string): cookie file path for query the proprietary 
                             KOA data
        
	format (string):  Output format: votable, ipac, csv, or tsv 
	                  (default: ipac)
        
	maxrec (integer):  maximum records to be returned 
	         default: -1 or not specified will return all requested records
        """

        debug = 0
        debugfname = ''

        if ('debugfile' in kwargs):
            
            debugfname = kwargs.get ('debugfile')

            if (len(debugfname) > 0):
      
                debug = 1

                logging.basicConfig (filename=debugfname, \
                    level=logging.DEBUG)
    
                with open (debugfname, 'w') as fdebug:
                    pass

            if debug:
                logging.debug ('')
                logging.debug ('debug turned on')
        
        if debug:
            logging.debug ('')
            logging.debug ('Enter query_datetime:')
      
#
#    modify baseurl if server keyword exists
#
        self.baseurl = conf.server

        if debug:
            logging.debug ('')
            logging.debug (f'baseurl (from conf)= {self.baseurl:s}')

        instrument = str(instrument)

        if (len(instrument) == 0):
            print ('Failed to find required parameter: instrument')
            return

        datetime = str(datetime)

        if (len(datetime) == 0):
            print ('Failed to find required parameter: datetime')
            return

        if (len(outpath) == 0):
            print ('Failed to find required parameter: outpath')
            return

        self.instrument = instrument
        self.datetime = datetime
        self.outpath = outpath

        if debug:
            logging.debug ('')
            logging.debug (f'instrument= {self.instrument:s}')
            logging.debug (f'datetime= {self.datetime:s}')
            logging.debug (f'outpath= {self.outpath:s}')

#
#    send url to server to construct the select statement
#
        param = dict()
        param['instrument'] = self.instrument
        param['datetime'] = self.datetime
       
        if debug:
            logging.debug ('')
            logging.debug ('call query_criteria')

        self.query_criteria (param, outpath, **kwargs)

        return
#
#} end Archive.query_datetime
#
 

    def query_date (self, instrument, date, outpath, **kwargs):
#
#{ Archive.query_date
#
        
        """
        'query_date' method searches KOA by 'date_obs' range
        
        Required Inputs:
        ---------------    
        instrument (string): HIRES

        date (string): a date_obs string in the format of 
            date1/date2 where '/' separates the two date values` 
            of format 'yyyy-mm-dd'

            the following inputs are acceptable:

            date1/: will search data with date later than (>=) 
                        date1
            
            /date2: will search data with date earlier than (<=)
                        date2

            date1: will search data with date equal to (=) date1

        outpath (string): a full output filepath of the returned metadata 
            table
    
        e.g. 
            instrument = 'hires',
            date = '2018-03-16/2018-03-18' 

        e.g. 
            instrument = 'hires',
            date = '2018-03-16/' 

        e.g. 
            instrument = 'hires',
            date = '/2018-03-18' 

        e.g. 
            instrument = 'hires',
            date = '2018-03-16' 

        Optional inputs:
	----------------
        cookiepath (string): cookie file path for querying the proprietary 
                             KOA data
        
	format (string):  Output format: votable, ipac, csv, or tsv 
	                  (default: ipac)
        
	maxrec (integer):  maximum records to be returned 
	         default: -1 or not specified will return all requested records
        """

        debug = 0
        debugfname = ''
        if (debug == 0):

            if ('debugfile' in kwargs):
            
                debug = 1
                debugfname = kwargs.get ('debugfile')

                if (len(debugfname) > 0):
      
                    logging.basicConfig (filename=debugfname, \
                        level=logging.DEBUG)
    
                    with open (debugfname, 'w') as fdebug:
                        pass

            if debug:
                logging.debug ('')
                logging.debug ('debug turned on')
        
        if debug:
            logging.debug ('')
            logging.debug ('')
            logging.debug ('Enter query_date:')
       
        instrument = str(instrument)

        if (len(instrument) == 0):
            print ('Failed to find required parameter: instrument')
            return

        date = str(date)

        if (len(date) == 0):
            print ('Failed to find required parameter: date')
            return

        if (len(outpath) == 0):
            print ('Failed to find required parameter: outpath')
            return

        self.instrument = instrument
        self.date = date
        self.outpath = outpath

        if debug:
            logging.debug ('')
            logging.debug (f'instrument= {self.instrument:s}')
            logging.debug (f'date= {self.date:s}')
            logging.debug (f'outpath= {self.outpath:s}')

#
#    send url to server to construct the select statement
#
        param = dict()
        param['instrument'] = self.instrument
        param['date'] = self.date
       
        if debug:
            logging.debug ('')
            logging.debug ('call query_criteria')

        self.query_criteria (param, outpath, **kwargs)

        return
#
#} end Archive.query_date
#


    def query_position (self, instrument, pos, outpath, **kwargs):
#
#{ Archive.query_position
#
        """
        'query_position' method searches KOA by 'position' 
        
        Required Inputs:
        ---------------    

        instrument (string): HIRES

        pos (string): a position string in the format of 
	
	1.  circle ra dec radius;
	
	2.  polygon ra1 dec1 ra2 dec2 ra3 dec3 ra4 dec4;
	
	3.  box ra dec width height;
	
	All RA Dec in decimal degree J2000 coordinate.
             
        e.g. 
            instrument = 'hires',
            pos = 'circle 230.0 45.0 0.5'

        outpath (string): a full filepath for the returned metadata table
        
        Optional Input:
        ---------------    
        cookiepath (string): cookie file path for querying the proprietary 
                             KOA data.
        
        format (string): votable, ipac, csv, tsv  (default: ipac)
	
	maxrec (integer):  maximum records to be returned 
	         default: -1 or not specified will return all requested records
        """
        
        debug = 0
        debugfname = ''
        if (debug == 0):

            if ('debugfile' in kwargs):
            
                debug = 1
                debugfname = kwargs.get ('debugfile')

                if (len(debugfname) > 0):
      
                    logging.basicConfig (filename=debugfname, \
                        level=logging.DEBUG)
    
                    with open (debugfname, 'w') as fdebug:
                        pass

            if debug:
                logging.debug ('')
                logging.debug ('debug turned on')
        
        if debug:
            logging.debug ('')
            logging.debug ('')
            logging.debug ('Enter query_position:')
      
        
        instrument = str(instrument)

        if (len(instrument) == 0):
            print ('Failed to find required parameter: instrument')
            return
 
        if (len(pos) == 0):
            print ('Failed to find required parameter: time')
            return

        if (len(outpath) == 0):
            print ('Failed to find required parameter: outpath')
            return

        self.instrument = instrument
        self.pos = pos
        self.outpath = outpath
 
        if debug:
            logging.debug ('')
            logging.debug (f'instrument=  {self.instrument:s}')
            logging.debug (f'pos=  {self.pos:s}')
            logging.debug (f'outpath= {self.outpath:s}')

#
#    send url to server to construct the select statement
#
        param = dict()
        param['instrument'] = self.instrument
        param['pos'] = self.pos

        self.query_criteria (param, outpath, **kwargs)

        return
#
#} end Archive.query_position
#
        

    def query_object (self, instrument, object, outpath, **kwargs):
#
#{ Archive.query_object
#
        
        """
        'query_object' method searches KOA by 'object name' 
        
        Required Inputs:
        ---------------    

        instrument: HIRES

        object (string): an object name resolvable by SIMBAD, NED, and
            ExoPlanet's name_resolve; 
       
        This method resolves the object name into coordiates to be used as the
	center of the circle position search with default radius of 0.5 deg.

        e.g. 
            instrument = 'hires',
            object = 'WD 1145+017'

        Optional Input:
        ---------------    
        cookiepath (string): cookie file path for query the proprietary 
                             KOA data.
        
	format (string):  Output format: votable, ipac, csv, tsv (default: ipac)

        radius (float): search radius in deg (default = 0.5 deg)

	maxrec (integer):  maximum records to be returned 
	         default: -1 or not specified will return all requested records
        """
        
        debug = 0
        debugfname = ''
        if (debug == 0):

            if ('debugfile' in kwargs):
            
                debug = 1
                debugfname = kwargs.get ('debugfile')

                if (len(debugfname) > 0):
      
                    logging.basicConfig (filename=debugfname, \
                        level=logging.DEBUG)
    
                    with open (debugfname, 'w') as fdebug:
                        pass

            if debug:
                logging.debug ('')
                logging.debug ('debug turned on')
        
        if debug:
            logging.debug ('')
            logging.debug ('')
            logging.debug ('Enter query_object_name:')

        instrument = str(instrument)

        if (len(instrument) == 0):
            print ('Failed to find required parameter: instrument')
            return
 
        if (len(object) == 0):
            print ('Failed to find required parameter: object')
            return

        if (len(outpath) == 0):
            print ('Failed to find required parameter: outpath')
            return

        self.instrument = instrument
        self.object = object
        self.outpath = outpath

        if debug:
            logging.debug ('')
            logging.debug (f'instrument= {self.instrument:s}')
            logging.debug (f'object= {self.object:s}')
            logging.debug (f'outpath= {self.outpath:s}')

        radius = 0.5 
        if ('radius' in kwargs):
            radius_str = kwargs.get('radius')
            radius = float(radius_str)

        if debug:
            logging.debug ('')
            logging.debug (f'radius= {radius:f}')

        """
        coords = None
        try:
            print (f'resolving object name')
 
            coords = name_resolve.get_icrs_coordinates (object)
        
        except Exception as e:

            if debug:
                logging.debug ('')
                logging.debug (f'name_resolve error: {str(e):s}')
            
            print (str(e))
            return

        ra = coords.ra.value
        dec = coords.dec.value
        
        if debug:
            logging.debug ('')
            logging.debug (f'ra= {ra:f}')
            logging.debug (f'dec= {dec:f}')
        
        self.pos = 'circle ' + str(ra) + ' ' + str(dec) \
            + ' ' + str(radius)
	
        """

        lookup = None
        try:
            if debug:
                lookup = objLookup (object, debug=1)
            else:
                lookup = objLookup (object)
        
            if debug:
                logging.debug ('')
                logging.debug ('objLookup run successful and returned')
        
        except Exception as e:

            if debug:
                logging.debug ('')
                logging.debug (f'objLookup error: {str(e):s}')
            
            print (str(e))
            return 

        if (lookup.status == 'error'):
            
            msg = 'Input object [' + object + '] lookup error: ' + \
                lookup.msg
            
            print (msg)
            return

        if debug:
            logging.debug ('')
            logging.debug (f'source= {lookup.source:s}')
            logging.debug (f'objname= {lookup.objname:s}')
            logging.debug (f'objtype= {lookup.objtype:s}')
            logging.debug (f'objdesc= {lookup.objdesc:s}')
            logging.debug (f'parsename= {lookup.parsename:s}')
            logging.debug (f'ra2000= {lookup.ra2000:s}')
            logging.debug (f'dec2000= {lookup.dec2000:s}')
            logging.debug (f'cra2000= {lookup.cra2000:s}')
            logging.debug (f'cdec2000= {lookup.cdec2000:s}')

       
        ra2000 = lookup.ra2000
        dec2000 = lookup.dec2000

        self.pos = 'circle ' + ra2000 + ' ' + dec2000 + ' ' + str(radius)
	
        if debug:
            logging.debug ('')
            logging.debug (f'pos= {self.pos:s}')
       
        print (f'object name resolved: ra= {ra2000:s}, dec={dec2000:s}')
 
 
#
#    send url to server to construct the select statement
#
        param = dict()
        
        param['instrument'] = self.instrument
        param['pos'] = self.pos

        self.query_criteria (param, outpath, **kwargs)

        return
#
#} end  Archive.query_object
#
        
    
    def query_criteria (self, param, outpath, **kwargs):
#
#{ Archive.query_criteria
#
        
        """
        'query_criteria' method allows searches of KOA by multiple
        parameters specified in a python dictionary (param).

        param: a dictionary containing a list of acceptable parameters:

            instrument (required): HIRES

            datetime (string): a datetime range string in the format of 
                datetime1/datetime2, '/' being the separator between first
                and second datetime valaues where datetime format is 
                'yyyy-mm-dd hh:mm:ss'
            
            date (string): a date range string in the format of 
                date1/date2, '/' being the separator between first
                and second date valaues where date format is 'yyyy-mm-dd'
            
            pos (string): a position string in the format of 
	
	        1.  circle ra dec radius;
	
	        2.  polygon ra1 dec1 ra2 dec2 ra3 dec3 ra4 dec4;
	
	        3.  box ra dec width height;
	
	        all RA Dec in decimal degree J2000 coordinate.
             
	    target (string): target name used in the project, this will be 
                searched against the database -- not SIMBAD or NED.

        outpath (string): file path for the returned metadata table 

        Optional parameters:
        --------------------
        cookiepath (string): cookie file path obtained via login method, only
                             required for querying the proprietary KOA data.
        
	format (string): output table format -- votable, ipac, csv, or tsv;
            default: ipac
	    
	maxrec (integer):  maximum records to be returned 
	         default: -1 or not specified will return all requested records
        """
        
        debug = 0
        debugfname = ''
        if (debug == 0):

            if ('debugfile' in kwargs):
            
                debug = 1
                debugfname = kwargs.get ('debugfile')

                if (len(debugfname) > 0):
      
                    logging.basicConfig (filename=debugfname, \
                        level=logging.DEBUG)
    
                    with open (debugfname, 'w') as fdebug:
                        pass

            if debug:
                logging.debug ('')
                logging.debug ('debug turned on')


#
#    during dev/test: if server keyword exists, modify baseurl
#
#    retrieve baseurl from conf class;
#
        self.baseurl = conf.server

        if ('server' in kwargs):
            self.baseurl = kwargs.get ('server')

        self.cgipgm = conf.cgipgm
        if ('cgipgm' in kwargs):
            self.cgipgm = kwargs.get ('cgipgm')

        if debug:
            logging.debug ('')
            logging.debug (f'baseurl= {self.baseurl:s}')
            logging.debug (f'cgipgm= {self.cgipgm:s}')
            logging.debug ('')
            logging.debug ('Enter query_criteria')
        
#
#    send url to server to construct the select statement
#
        self.outpath = outpath
 
        len_param = len(param)

        if debug:
            logging.debug ('')
            logging.debug (f'outpath= {self.outpath:s}')
            
            logging.debug ('')
            logging.debug (f'len_param= {len_param:d}')

            for k,v in param.items():
                logging.debug (f'k, v= {k:s}, {str(v):s}')

        self.cookiepath = ''
        if ('cookiepath' in kwargs): 
            self.cookiepath = kwargs.get('cookiepath')

        if debug:
            logging.debug ('')
            logging.debug (f'cookiepath= {self.cookiepath:s}')

        self.format ='ipac'
        if ('format' in kwargs): 
            self.format = kwargs.get('format')

        self.maxrec = -1 
        if ('maxrec' in kwargs): 
            self.maxrec = kwargs.get('maxrec')
        

#        datatype = type (self.maxrec).__name__
#        print (f'datatype= {datatype:s}')

        try:
            self.maxrec = float(self.maxrec)
            self.maxrec = int(self.maxrec)
        except Exception as e:
            print (f'Failed to convert maxrec: ' + str(self.maxrec) + \
                ' to integer.')
            return

        if debug:
            logging.debug ('')
            logging.debug (f'format= {self.format:s}')
            logging.debug (f'maxrec= {self.maxrec:d}')

        data = urllib.parse.urlencode (param)


#
#    urls for nph-tap.py, nph-koaLogin, nph-makeQyery, 
#    nph-getKoa, and nph-getCaliblist
#
        self.tap_url = self.baseurl + self.cgipgm
        
        self.makequery_url = self.baseurl + 'cgi-bin/KoaAPI/nph-makeQuery?'

        if debug:
            logging.debug ('')
            logging.debug (f'tap_url= [{self.tap_url:s}]')
            logging.debug (f'makequery_url= [{self.makequery_url:s}]')


        url = self.makequery_url + data            

        if debug:
            logging.debug ('')
            logging.debug (f'url= {url:s}')

        query = ''
        try:
            query = self.__make_query (url) 

            if debug:
                logging.debug ('')
                logging.debug ('returned __make_query')
  
        except Exception as e:

            if debug:
                logging.debug ('')
                logging.debug (f'Error: {str(e):s}')
            
            print (str(e))
            return 
        
        if debug:
            logging.debug ('')
            logging.debug (f'query= {query:s}')
       
        self.query = query

#
#    send tap query
#
        self.tap = None
        if (len(self.cookiepath) > 0):
            
            if debug:
                logging.debug ('')
                logging.debug (f'cookiepath= {self.cookiepath:s}')
       
            if debug:
                
                try:
                    self.tap = KoaTap (self.tap_url, \
                        format=self.format, \
                        maxrec=self.maxrec, \
                        cookiefile=self.cookiepath, \
	                debug=1)
                
                except Exception as e:
            
                    if debug:
                        logging.debug ('')
                        logging.debug (f'Error: {str(e):s}')
                    
                    print (str(e))
                    return 

            else:
                try:
                    self.tap = KoaTap (self.tap_url, \
                        format=self.format, \
                        maxrec=self.maxrec, \
                        cookiefile=self.cookiepath)
                
                except Exception as e:
            
                    if debug:
                        logging.debug ('')
                        logging.debug (f'Error: {str(e):s}')
                    
                    print (str(e))
                    return 
        
        else: 
            if debug:
                try:
                    self.tap = KoaTap (self.tap_url, \
                        format=self.format, \
                        maxrec=self.maxrec, \
	                debug=1)
                
                except Exception as e:
            
                    if debug:
                        logging.debug ('')
                        logging.debug (f'Error: {str(e):s}')
                    
                    print (str(e))
                    return 
        
            else:
                try:
                    self.tap = KoaTap (self.tap_url, \
                        format=self.format, \
                        maxrec=self.maxrec)
        
                except Exception as e:
            
                    if debug:
                        logging.debug ('')
                        logging.debug (f'Error: {str(e):s}')
                    
                    print (str(e))
                    return 
        
        if debug:
            logging.debug('')
            logging.debug('koaTap initialized')
            logging.debug('')
            logging.debug(f'query= {query:s}')

        print ('submitting request...')

        if debug:
            logging.debug('')
            logging.debug('call self.tap.send_async with debug')
            
            retstr = self.tap.send_async (query, \
                outpath=self.outpath, \
                format=self.format, \
                maxrec=self.maxrec, debug=1)
        else:
            logging.debug('')
            logging.debug('call self.tap.send_async NO debug')
            
            retstr = self.tap.send_async (query, \
                outpath=self.outpath, \
                format=self.format, \
                maxrec=self.maxrec)
        
        if debug:
            logging.debug ('')
            logging.debug (f'return self.tap.send_async:')
            logging.debug (f'retstr= {retstr:s}')

        retstr_lower = retstr.lower()

        indx = retstr_lower.find ('error')
    
#        if debug:
#            logging.debug ('')
#            logging.debug (f'indx= {indx:d}')

        if (indx >= 0):
            print (retstr)
            return
            #sys.exit()

#
#    no error: 
#
        print (retstr)
        return
#
#} end Archive.query_criteria
#
        
    
    def query_adql (self, query, outpath, **kwargs):
#
#{ Archive.query_adql
#
       
        """
        'query_adql' method receives a qualified ADQL query string from
	the user input.
        
        Required Inputs:
        ---------------    
            query (string):  an ADQL query

            outpath (string): the output filename of the returned metadata table
        
        Optional inputs:
	----------------
            cookiepath (string): cookie file path for query the proprietary 
                                 KOA data.
        
	    format (string):  output format: votable, ipac, csv, or tsv 
	             (default: ipac)
        
	    maxrec (integer):  maximum records to be returned 
	         default: -1 or not specified will return all requested records
        """
  
        debug = 0
        debugfname = ''
        if (debug == 0):

            if ('debugfile' in kwargs):
            
                debug = 1
                debugfname = kwargs.get ('debugfile')

                if (len(debugfname) > 0):
      
                    logging.basicConfig (filename=debugfname, \
                        level=logging.DEBUG)
    
                    with open (debugfname, 'w') as fdebug:
                        pass

            if debug:
                logging.debug ('')
                logging.debug ('debug turned on')
        
#
#    retrieve baseurl from conf class;
#
#    during dev or test, baseurl will be a keyword input
#
        self.baseurl = conf.server
        if ('server' in kwargs):
            self.baseurl = kwargs.get ('server')

        self.cgipgm = conf.cgipgm
        if ('cgipgm' in kwargs):
            self.cgipgm = kwargs.get ('cgipgm')

        if debug:
            logging.debug ('')
            logging.debug (f'baseurl= {self.baseurl:s}')
            logging.debug (f'cgipgm= {self.cgipgm:s}')
            logging.debug ('')
            logging.debug ('Enter query_adql:')
        
        if (len(query) == 0):
            print ('Failed to find required parameter: query')
            return
        
        if (len(outpath) == 0):
            print ('Failed to find required parameter: outpath')
            return
        
        self.query = query
        self.outpath = outpath
 
        if debug:
            logging.debug ('')
            logging.debug ('')
            logging.debug (f'query= {self.query:s}')
            logging.debug (f'outpath= {self.outpath:s}')
       
        self.cookiepath = '' 
        if ('cookiepath' in kwargs): 
            self.cookiepath = kwargs.get('cookiepath')

        if debug:
            logging.debug ('')
            logging.debug (f'cookiepath= {self.cookiepath:s}')

        self.format = 'ipac'
        if ('format' in kwargs): 
            self.format = kwargs.get('format')

        self.maxrec = -1 
        if ('maxrec' in kwargs): 
            self.maxrec = kwargs.get('maxrec')
        
        self.propflag = 1 
        if ('propflag' in kwargs): 
            self.propflag = kwargs.get('propflag')
        
        if debug:
            logging.debug ('')
            logging.debug (f'format= {self.format:s}')
            logging.debug (f'maxrec= {self.maxrec:d}')
            logging.debug (f'propflag= {self.propflag:d}')


#
#    urls for nph-tap.py
#
        self.tap_url = self.baseurl + self.cgipgm
        
        if debug:
            logging.debug ('')
            logging.debug (f'tap_url= [{self.tap_url:s}]')

#
#    send tap query
#
        self.tap = None

        if (len(self.cookiepath) > 0):
           
            if debug:
                self.tap = KoaTap (self.tap_url, \
                    format=self.format, \
                    maxrec=self.maxrec, \
                    cookiefile=self.cookiepath, \
	            debug=1)
            else:
                self.tap = KoaTap (self.tap_url, \
                    format=self.format, \
                    maxrec=self.maxrec, \
                    cookiefile=self.cookiepath)
        else: 
            if debug:
                self.tap = KoaTap (self.tap_url, \
                    format=self.format, \
                    maxrec=self.maxrec, \
	            debug=1)
                
                #self.tap = KoaTap (self.tap_url, \
                #    format=self.format, \
                #    maxrec=self.maxrec, \
                #    propflag=self.propflag, \
	        #    debug=1)
            else:
                self.tap = KoaTap (self.tap_url, \
                    format=self.format, \
                    propflag=self.propflag, \
                    maxrec=self.maxrec)
        
        if debug:
            logging.debug('')
            logging.debug('koaTap initialized')
            logging.debug(f'query= {query:s}')
            logging.debug('call self.tap.send_async')

        print ('submitting request...')

        if debug:
            if (len(self.outpath) > 0):
                retstr = self.tap.send_async (query, \
                    outpath=self.outpath, \
                    format=self.format, \
                    maxrec=self.maxrec, \
                    debug=1)
                
                #retstr = self.tap.send_async (query, \
                #    outpath=self.outpath, \
                #    format=self.format, \
                #    maxrec=self.maxrec, \
                #    propflag=self.propflag, \
                #    debug=1)
            else:
                retstr = self.tap.send_async (query, \
                    format=self.format, \
                    maxrec=self.maxrec, \
                    debug=1)
        else:
            if (len(self.outpath) > 0):
                retstr = self.tap.send_async (query, \
                    outpath=self.outpath, \
                    format=self.format, \
                    maxrec=self.maxrec)
            else:
                retstr = self.tap.send_async (query, \
                    format=self.format, \
                    maxrec=self.maxrec)
        
        if debug:
            logging.debug ('')
            logging.debug (f'return self.tap.send_async:')
            logging.debug (f'retstr= {retstr:s}')

        retstr_lower = retstr.lower()

        indx = retstr_lower.find ('error')
    
        if (indx >= 0):
            print (retstr)
            return
            #sys.exit()

#
#    no error: 
#
        print (retstr)
        return
#
#} end Archive.query_adql
#


    def query_moving_object (self, **kwargs):
#
#{ Archive.query_moving_object
#
        """
        'query_moving_object' method searches KOA for moving object. 
        Due to the high number of input parameters, all inputs are made 
        to be keyword parameters.
        
        Required Inputs:
        ---------------    
        instrument (string): one of KOA instruments e.g. nirc2, nirspec, etc..;
        
        object (string): moving object to search e.g. pluto, jupiter, etc.. 

        startdate (string): a date string in the format of 'yyyy-mm-dd'

        outdir (string): a output directory for the returned output files: 
        
        outfile (string): output result list file name (a json file),
        
        Output files contains the following:

            --  a json formated result lists that contains the information of 
                the moving objects found including:
            
                input parameters,
                numbers of moving objects found,
                metadata,
                URL link to the file that is the same metadata in IPAC format,
                URL link to the PNG files showing the intersections of the KOA 
                observation with the moving objects' trajectory.
            
            -- the metadata files associated the moving objects.


        Optional inputs:
	----------------
        enddate (string): a date string in the format of 'yyyy-mm'dd';
            if missing the default is the current date,
            
        naifid (integer): JPL's NAIF object ID number to more precisely 
            identify the moving object; this is needed when the object name
            such as 'pluto' which results in multiple targets.
        
        cookiepath (string): a cookie file path obtained via login method, only
                             required for querying the proprietary KOA data.
        
        datatype (string): "image", "spec", or "both" -- to search for either
            the image data, the spectra data, or both; default is "both",
        
        graphoption (integer): 0 or 1 -- indicates whether to create graphic 
            png files of moving object tracjectories; default is 1,

        orbitalinput (int): 0 or 1 indicates whether the precise orbital 
            input parameters are provided; default is 0,
       
        The orbital input contains the following parameters:
    
            epoch (mjd),  
	    ec (eccentricity),
	    om (deg): longitude of ascending node, 
	    w (deg) : argument of perihelion,
	    inc (deg): inclination,
	  
            qr (au) (perihelion distance -- for Comet),
	    tp (JD) (perihelion Julian date -- for Comet),
        
            or 
        
            a (au):   semi-major axis -- for Asteroid,
	    m0 (deg):  mean anomaly -- for Asteroid


        where the notations are consistent with the inputs to the JPL's url
        for obtaining bsp file which constains the object's SPK ID:

	    -EPOCH: epoch, 
	    -EC:    eccentricity,
	    -OM:    longitude of ascending node,
            -W:     argument of perihelion,	
	    -INC:    inclination,
	    -QR:    perihelion distance,
	    -TP:    perihelion Julian date,

        
        Calling synopsis:
	----------------
        e.g. 
            Koa.query_moving_object ( \
                instrument = 'nirspec', \
                object = 'pluto', \
                startdate = '1995-01-01', \
                enddate = '2020-07-08', \
                outdir = './nirspec_pluto', \
                outfile = 'nirspec_pluto.json', \
                naifid = '999', \
                datatype = 'both', \
                graphoption = 1)
        """
        debug = 0
        debugfname = ''
        if ('debugfile' in kwargs):
            
            debug = 1
            debugfname = kwargs.get ('debugfile')

            if (len(debugfname) > 0):
      
                logging.basicConfig (filename=debugfname, \
                    level=logging.DEBUG)
    
                with open (debugfname, 'w') as fdebug:
                    pass

#
#    retrieve baseurl from conf class;
#
#    during dev or test, baseurl will be a keyword input
#
        self.baseurl = conf.server

        if ('server' in kwargs):
            self.baseurl = kwargs.get ('server')

        if debug:
            logging.debug ('')
            logging.debug ('Enter query_moving_object:')
            logging.debug (f'baseurl= {self.baseurl:s}')
       
        instrument = ''
        if ('instrument' in kwargs): 
            instrument = str(kwargs.get('instrument'))

        if (len(instrument) == 0):
            print ('Failed to find required parameter: instrument')
            return

        object = ''
        if ('object' in kwargs): 
            object = str(kwargs.get('object'))

        if (len(object) == 0):
            print ('Failed to find required parameter: object')
            return

        outdir = ''
        if ('outdir' in kwargs): 
            outdir = str(kwargs.get('outdir'))
        
        if (len(outdir) == 0):
            print ('Failed to find required parameter: outdir')
            return
        
        outfile = ''
        if ('outfile' in kwargs): 
            outfile = str(kwargs.get('outfile'))
        
        if (len(outfile) == 0):
            print ('Failed to find required parameter: outfile')
            return
        
        startdate = ''
        if ('startdate' in kwargs): 
            startdate = str(kwargs.get('startdate'))

        if (len(startdate) == 0):
            print ('Failed to find required parameter: startdate')
            return

        enddate = ''
        if ('enddate' in kwargs): 
            enddate = str(kwargs.get('enddate'))

        if (len(enddate) == 0):
            today = date.today()
            enddate = today.strftime ("%Y-%m-%d")
        
            if debug:
                logging.debug ('')
                logging.debug (f'today= {enddate:s}')

        if debug:
            logging.debug ('')
            logging.debug (f'instrument= {instrument:s}')
            logging.debug (f'object= {object:s}')
            logging.debug (f'outdir= {outdir:s}')
            logging.debug (f'outfile= {outfile:s}')
            logging.debug (f'startdate= {startdate:s}')
            logging.debug (f'enddate= {enddate:s}')

        cookiepath = ''
        if ('cookiepath' in kwargs): 
            cookiepath = kwargs.get('cookiepath')

        naifid = '' 
        if ('naifid' in kwargs): 
            naifid = str (kwargs.get('naifid'))

        datatype = 'both'
        if ('datatype' in kwargs): 
            datatype = str (kwargs.get('datatype'))

        graphoption = 1 
        if ('graphoption' in kwargs): 
            graphoption = int (kwargs.get('graphoption'))

        orbitalinput = 0 
        if ('orbitalinput' in kwargs): 
            orbitalinput = int (kwargs.get('orbitalinput'))

        if debug:
            logging.debug ('')
            logging.debug (f'cookiepath= {cookiepath:s}')
            logging.debug (f'naifid= {naifid:s}')
            logging.debug (f'datatype= {datatype:s}')
            logging.debug (f'graphoption= {graphoption:d}')
            logging.debug (f'orbitalinput= {orbitalinput:d}')

        epoch = ''
        ecstr = ''
        omstr = ''
        wstr = ''
        instr = ''
        qrstr = ''
        tpstr = ''
        astr = ''
        m0str = ''

        if (orbitalinput == 1):

            if ('epoch' in kwargs): 
                epoch = kwargs.get('epoch')

            if ('ec' in kwargs): 
                ecstr = kwargs.get('ec')

            if ('om' in kwargs): 
                omstr = kwargs.get('om')

            if ('w' in kwargs): 
                wstr = kwargs.get('w')

            if ('inc' in kwargs): 
                instr = kwargs.get('inc')

            if ('qr' in kwargs): 
                qrstr = kwargs.get('qr')

            if ('tp' in kwargs): 
                tpstr = kwargs.get('tp')

            if ('a' in kwargs): 
                astr = kwargs.get('a')

            if ('m0' in kwargs): 
                m0str = kwargs.get('m0')

            if debug:
                logging.debug ('')
                logging.debug (f'epoch= {epoch:s}')
                logging.debug (f'ecstr= {ecstr:s}')
                logging.debug (f'omstr= {omstr:s}')
                logging.debug (f'wstr= {wstr:s}')
                logging.debug (f'instr= {instr:s}')
                logging.debug (f'qrstr= {qrstr:s}')
                logging.debug (f'tpstr= {tpstr:s}')
                logging.debug (f'astr= {astr:s}')
                logging.debug (f'm0str= {m0str:s}')


        moss_url = self.baseurl + 'cgi-bin/MossAPI/nph-mossSearch?'

        param = dict()
        param['instrument'] = instrument
        param['target'] = object
        param['starttime'] = startdate
        param['endtime'] = enddate

        if (len(naifid) > 0):
            param['naifid'] = naifid

        if ((instrument.lower() == 'hires') and (datatype.lower() == 'both')):
            datatype = 'spec'

        if (len(datatype) > 0):
            param['datatype'] = datatype
        
        param['graphoption'] = graphoption 
        
        param['orbitalinput'] = orbitalinput
        
        if (orbitalinput == 1):

            param['epoch'] = epoch
            param['ec'] = ecstr
            param['om'] = omstr
            param['w'] = wstr
            param['in'] = instr
            param['qr'] = qrstr
            param['tp'] = tpstr
            param['a'] = astr
            param['m0'] = m0str

#
#    These two parameters are for development debugging during development only;
#    take them out before release.
#
        workspace = ''
        if ('workspace' in kwargs): 
            workspace = kwargs.get('workspace')

        if debug:
            logging.debug ('')
            logging.debug (f'workspace= {workspace:s}')

        data = urllib.parse.urlencode (param)

        url = moss_url + data 

        if debug:
            logging.debug ('')
            logging.debug (f'moss full url sent to server:')
            logging.debug (f'url= {url:s}')

#
#    load cookie
#
        cookiejar = None
        
        if (len(cookiepath) > 0):
   
            cookiejar = http.cookiejar.MozillaCookieJar (cookiepath)

            try: 
                cookiejar.load (ignore_discard=True, ignore_expires=True)
    
                if debug:
                    logging.debug (\
                        f'cookie loaded from file: {cookiepath:s}')
        
                for cookie in cookiejar:
                    
                    if debug:
                        logging.debug ('')
                        logging.debug ('cookie=')
                        logging.debug (cookie)
                        logging.debug (f'cookie.name= {cookie.name:s}')
                        logging.debug (f'cookie.value= {cookie.value:s}')
                        logging.debug (f'cookie.domain= {cookie.domain:s}')

            except Exception as e:
                if debug:
                    logging.debug ('')
                    logging.debug (f'loadCookie exception: {str(e):s}')
                pass


#
#    if 'outdir' doesn't exist, create the directory,
#
#    decimal mode work for both python2.7 and python3;
#    0755 also works for python 2.7 but not python3
#    convert octal 0775 to decimal: 493
#
        d1 = int ('0775', 8)
        
        if debug:
            logging.debug ('')
            logging.debug (f'd1= {d1:d}')
                                                 
        try:
            os.makedirs (outdir, mode=d1, exist_ok=True)
        
        except Exception as e:
            print (msg)
            return
        
        if debug:
            logging.debug ('')
            logging.debug ('returned os.makedirs')

        outpath = outdir + '/' + outfile

#
#    send query in async mode and loop until done
#
        print ('submitting request...')

        response = None
        try:
            if (cookiejar is not None):
        
                response = requests.post (moss_url, data=param, \
	            cookies=cookiejar, allow_redirects=False)
                
                if debug:
                    logging.debug ('')
                    logging.debug ('request sent with cookiejar')

            else: 
                response = requests.post (moss_url, data= param, \
	            allow_redirects=False)

                if debug:
                    logging.debug ('')
                    logging.debug ('request sent without cookiejar')

        except Exception as e:
           
            status = 'error'
            msg = str(e)
	    
            if debug:
                logging.debug ('')
                logging.debug (f'exception: e= {str(e):s}')
           

#
# {  if response is a json structure: parse for status;
#    or if it is a HTTP error, then it is text: just print the message
#
        jsondata = None
        status = ''
        msg = ''
           
        content_type = ''
        try:
            content_type = response.headers['Content-type']
        except Exception as e:

            if debug:
                logging.debug ('')
                logging.debug (f'exception extract content-type: {str(e):s}')

        if debug:
            logging.debug ('')
            logging.debug (f'content_type= {content_type:s}')

        
        if (content_type == 'application/json'):
                
            if debug:
                logging.debug ('')
                logging.debug ('case json errmsg:')
      
            try:
                jsondata = response.json()
                status = jsondata['status']
                    
            except Exception as e:
                
                if debug:
                    logging.debug ('')
                    logging.debug (f'JSON object parse error: {str(e):s}')
      
                status = 'error'
                msg = 'JSON parse error: ' + str(e)
                
                if debug:
                    logging.debug ('')
                    logging.debug (f'status= {status:s}')
                    logging.debug (f'msg= {msg:s}')

                print (response.text)
                return


        else:
            print ('response:')
            print (response)
            if (response is not None):
                print (response.text)
            return

        if debug:
            logging.debug ('')
            logging.debug ('here')
            logging.debug (f'status= {status:s}')


#
#} end extract status from json structure
#

        if (status.lower() == 'error'):
#
#    retrieve error message and return
#
            msg = jsondata['msg']
            print (msg)
            return
        
        elif (status.lower() == 'executing'):
#
#    retrieve statusurl, and retrieve status in a loop 
#    until status is 'completed' or 'error'
#
            statusurl = jsondata['statusurl']
            
            while ((status.lower() != 'completed') and \
                (status.lower() != 'error')):
                
                time.sleep (2)
                jsondata = self.__get_moss_status (statusurl, debug=1)
       
                status = jsondata['status']

                if debug:
                    logging.debug ('')
                    #logging.debug (f'jsondata= ')
                    #logging.debug (jsondata)
                    logging.debug (f'status= {status:s}')
             
        if debug:
            logging.debug ('')
            logging.debug (f'out of while loop: status= {status:s}')
       
        
        if (status.lower() == 'error'):
#
#    retrieve error message and return
#
            msg = jsondata['msg']
            print (msg)
            return
        
        elif (status.lower() == 'completed'):
#
#    retrieve resulturl and retrieve resultfile
#
            resulturl = jsondata['resulturl']
        
            if debug:
                logging.debug ('')
                logging.debug (f'XXX> resulturl= {resulturl:s}')
       
            try:
                if debug:
                    logging.debug ('')
                    logging.debug ('')
                    logging.debug ('')
                    logging.debug (f'XXX (before)> resulturl= {resulturl:s}')

                self.__get_moss_resultfile (resulturl, outpath) 
                
                if debug:
                    logging.debug ('')
                    logging.debug (f'XXX (after) > resulturl= {resulturl:s}')
                    logging.debug ('')
                    logging.debug ('')
                    logging.debug ('')

                    logging.debug ('returned __get_moss_resultfile')
             
            except Exception as e:
           
                if debug:
                    logging.debug ('')
                    logging.debug (
                        f'Exception error get_moss_resultfile: {str(e):s}')
                print (str(e))
                return

        msg = 'result metafile list written to ' + outpath
        print (msg)

#
#    download metadata files
#
        try:
            if debug:
                logging.debug ('')
                logging.debug ('call __download_moving_object_metadata')
             
            #self.__download_moving_object_metadata (outpath, outdir, debug=1)
            self.__download_moving_object_metadata (outpath, outdir)
                
            if debug:
                logging.debug ('')
                logging.debug ('returned __download_moving_object_metadata')
             
        except Exception as e:
           
            if debug:
                logging.debug ('')
                logging.debug (
                    f'Exception error get_moss_resultfile: {str(e):s}')
            print (str(e))
            return

        return       
#
#} end Archive.query_moving_object
#

    
    def __download_moving_object_metadata (self, jsonpath, outdir, **kwargs):
#
#{ Archive.download_moving_object_metadata
#
        """
        'download_moving_object_metadata' method downloads moving object 
        search's result metadata tables files and their optional associated 
        graph files.
        
        The result metadata tables contain the parameters and URL links of 
        KOA's data; this table can be used as the input to 'download' method
        for downloading KOA moving objects.
        
        The graph metadata table contains the information of these KOA data 
        detected in the moving object's trajectory and the links to the graph 
        PNG files for visualization.
    
        The result metadata tables are all in IPAC ascii format.

        Required input:
	----------------
	jsonpath (string): The full path of result json file obtained from 
            running 'query_moving_object' methods,    
       
        outdir (string): directory for the returned files,
        
        Optional input:
	----------------
        pngflag (0 or 1): 1 indicating that PNG files will be downloaded; 
                          default is 1.
        """

        debug = 0
        if ('debug' in kwargs):
            debug = 1
            
        if debug:
            logging.debug ('')
            logging.debug ('Enter download_moving_object_metadata')
            logging.debug (f'jsonpath= {jsonpath:s}')
            logging.debug (f'outdir= {outdir:s}')
            logging.debug (f'self.baseurl= {self.baseurl:s}')

        baseurl = ''
        len_baseurl = len(self.baseurl)
        if (self.baseurl[len_baseurl-1] == '/'):
            baseurl = self.baseurl[0:len_baseurl-1]
        else:
            baseurl = self.baseurl

        if debug:
            logging.debug ('')
            logging.debug (f'baseurl= {baseurl:s}') 


        pngflag = 1 
        if ('pngflag' in kwargs):
            pngflag = int(kwargs.get ('pngflag'))

        if debug:
            logging.debug ('')
            logging.debug (f'pngflag= {pngflag:d}')

#
#    parse input json file for parameters
#
        fp = None
        jsondata = None
        try:
            if debug:
                logging.debug ('')
                logging.debug ('here0-0') 

            with open (jsonpath) as fp:

                if debug:
                    logging.debug ('')
                    logging.debug ('here0-1') 

                jsondata = json.load (fp)
            
                if debug:
                    logging.debug ('')
                    logging.debug ('here0-2') 
        
            if debug:
                logging.debug ('')
                logging.debug ('here0-3') 
            
            fp.close()

            if debug:
                logging.debug ('')
                logging.debug ('here0-4') 
            
        except Exception as e:

            if debug:
                logging.debug ('')
                logging.debug ('here1-0') 
            
            if (fp is not None):
                fp.close()
            
            if debug:
                logging.debug ('')
                logging.debug ('here1-1') 

            msg = 'Failed to read input JSON file: ' + jsonpath
            print (msg)
            if debug:
                logging.debug ('')
                logging.debug ('here1-2') 

            raise Exception (msg) 
            
        
        if debug:
            logging.debug ('')
            logging.debug ('jsondata: ') 
            logging.debug (jsondata) 

        urlprefix = jsondata['urlprefix']
        if debug:
            logging.debug ('')
            logging.debug (f'urlprefix= {urlprefix:s}') 

        results = jsondata['results']
        if debug:
            logging.debug ('')
            logging.debug ('results: ') 
            logging.debug (results) 

        nresulttbl = int(results['nresulttbl'])
        
        if (nresulttbl == 0):
            print ("There is no result table in this MOSS search.")
            raise Exception (msg) 
            

        ngraphtbl = 0
        if (nresulttbl > 0):
            ngraphtbl = int(results['ngraphtbl'])
        
        if debug:
            logging.debug ('')
            logging.debug (f'nresulttbl= {nresulttbl:d}') 
            logging.debug (f'ngraphtbl= {ngraphtbl:d}') 

#
#    download result metadata tables: get rid of the last '/' from baseurl
#

        nmeta_total = nresulttbl + ngraphtbl  
        msg = 'Start downloading ' +  str(nmeta_total) + ' metadata tables '
        
        if (pngflag and ngraphtbl > 0):
            msg = msg + 'and their associated graph PNG files ' 
        msg = msg + 'you requested;'
        
        print (msg)
        print (f'please check your outdir: {outdir:s} for  progress.')

        ndnloaded_metatbl = 0
        ndnloaded_png = 0

        if (nresulttbl > 0):

#
# { download result metadata tables
#
            for l in range (nresulttbl):
           
                fileurl = jsondata['results']['resulttbls'][l]['fileurl']
        
                if debug:
                    logging.debug ('')
                    logging.debug (f'fileurl= {fileurl:s}') 

                resultfile = ''
                ind = fileurl.rfind ('/')
                if (ind >= 0):
                    resultfile = fileurl[ind+1:]
                
                resultpath = outdir + '/' + resultfile

                if debug:
                    logging.debug ('')
                    logging.debug (f'resultfile= {resultfile:s}') 
                    logging.debug (f'resultpath= {resultpath:s}') 

                url = baseurl + fileurl
            
                if debug:
                    logging.debug ('')
                    logging.debug (f'url= {url:s}') 

                try:
                    self.__get_moss_resultfile (url, resultpath, debug=1)
                
                    if debug:
                        logging.debug ('')
                        logging.debug ('returned __get_moss_resultfile') 

                    #msg = 'Result metadata table downloaded to file [' + \
                    #    resultpath + ']'
                    #print (msg)
                    
                    ndnloaded_metatbl = ndnloaded_metatbl + 1 
                
                except Exception as e:

                    if debug:
                        logging.debug ('')
                        logging.debug (f'get resultfile exception: {str(e):s}') 

#
# } end download result metadata tables
#

        if (ngraphtbl > 0):
#
#{  download graph metadata tables: make png subdir
#
            pngsubdir = ''
            if pngflag:
                pngsubdir = outdir + '/png'

                d1 = int ('0775', 8)

                if debug:
                    logging.debug ('')
                    logging.debug (f'd1= {d1:d}')
    
#
#    a png file for each moss run have different file name (pid at the end)
#    so need to have an empty png subdirectory for each run.
#
                isExist = os.path.exists (pngsubdir)
            
                if debug:
                    logging.debug ('')
                    logging.debug (f'pngsubdir isExist=  {isExist}')
           
                if (isExist):
                    for f in os.listdir (pngsubdir):
                        os.remove (os.path.join (pngsubdir, f))
#
#    create pngsubdir if the subdir doesn't exist
#
                try:
                    os.makedirs (pngsubdir, mode=d1, exist_ok=True) 

                except Exception as e:
            
                    msg = f'Failed to create {pngsubdir:s}: {str(e):s}'
                    print (msg)
                    raise Exception (msg) 
            

                if debug:
                    logging.debug ('')
                    logging.debug ('returned os.makedirs: pngsubdir') 


            for l in range (ngraphtbl):
           
                fileurl = \
                    jsondata['results']['graphtbls'][l]['graphfileurl']
        
                if debug:
                    logging.debug ('')
                    logging.debug (f'fileurl= {fileurl:s}') 

                graphfile = ''
                ind = fileurl.rfind ('/')
                if (ind >= 0):
                    graphfile = fileurl[ind+1:]
                
                graphpath = outdir + '/' + graphfile

                if debug:
                    logging.debug ('')
                    logging.debug (f'graphfile= {graphfile:s}') 
                    logging.debug (f'graphpath= {graphpath:s}') 

                url = baseurl + fileurl
            
                if debug:
                    logging.debug ('')
                    logging.debug (f'url= {url:s}') 

                try:
                    self.__get_moss_resultfile (url, graphpath, debug=1)
                
                    if debug:
                        logging.debug ('')
                        logging.debug ('returned __get_moss_resultfile') 

                    #msg = 'Graphic metadata table downloaded to file [' + \
                    #    graphpath + ']'
                    #print (msg)
                    
                    ndnloaded_metatbl = ndnloaded_metatbl + 1 
                
                except Exception as e:

                    if debug:
                        logging.debug ('')
                        logging.debug (f'get graphfile exception: {str(e):s}') 

#
#    if pngflag = 1: download graphic PNG files
#
                if pngflag:

                    ind = -1
                    ind = url.rfind('/')
                    
                    url_prefix = url[0:ind]
                    
                    if debug:
                        logging.debug ('')
                        logging.debug ('hrere0')
                        logging.debug (f'url_prefix= {url_prefix:s}') 

                    nrecstr = jsondata['results']['graphtbls'][l]['nrec']
                    nrec_png = int(nrecstr)

                    if debug:
                        logging.debug ('')
                        logging.debug (f'nrec_png= {nrec_png:d}') 
                   
                    
                    for ipng in range (nrec_png):

                        pngfile = \
                            jsondata['results']['graphtbls'][l]['metadata'][ipng]['pngfile']
                        
                        pngurl = url_prefix + '/' + pngfile
                        pngpath = pngsubdir + '/' + pngfile


                        if debug:
                            logging.debug ('')
                            logging.debug (f'ipng= {ipng:d}')
                            logging.debug (f'pngfile= {pngfile:s}') 
                            logging.debug (f'pngpath= {pngpath:s}') 
                            logging.debug (f'pngurl= {pngurl:s}') 

                        try:
                            self.__get_moss_resultfile (pngurl, pngpath)
                
                            if debug:
                                logging.debug ('')
                                logging.debug ('returned __get_moss_resultfile') 
                            ndnloaded_png = ndnloaded_png + 1 
                
                        except Exception as e:

                            if debug:
                                logging.debug ('')
                                logging.debug (\
                                    f'get pngfile exception: {str(e):s}') 
                            msg = f'get pngfile exception: {str(e):s}' 
                            raise Exception (msg) 
            
     
        if (ngraphtbl == 0):
            print (f'{ndnloaded_metatbl:d} metadata tables downloaded.')
        elif (ngraphtbl > 0):
            print (f'{ndnloaded_metatbl:d} metadata tables and {ndnloaded_png:d} graph PNG files downloaded.') 
            
#
# } end download graph metadata tables
#

        return

#
#} end Archive.download_moving_object_metadata
#

    
    def __get_moss_resultfile (self, resulturl, outpath, **kwargs):
#
#{ Archive.__get_moss_resultfile
#
        response = None
        status = ''
        msg = ''
        
        debug = 0 

        if ('debug' in kwargs):
            debug = int(kwargs.get ('debug'))

        if debug:
            logging.debug ('')
            logging.debug ('Enter __get_moss_resultfile:')
            logging.debug (f'XXX> resulturl= {resulturl:s}')
            logging.debug (f'outpath= {outpath:s}')


#
#   send resulturl to retrieve result table
#
        try:
            response = requests.get (resulturl, stream=True)
        
            if debug:
                logging.debug ('')
                logging.debug ('resulturl request sent')

        except Exception as e:
           
            status = 'error'
            msg = str(e)
	    
            if debug:
                logging.debug ('')
                logging.debug (f'exception: e= {str(e):s}')
            
            raise Exception (msg)    
     
       
#
# save table to file
#
        if debug:
            logging.debug ('')
            logging.debug ('save data to outpath')

        try:
            fp = open (outpath, "wb")
        
        except Exception as e:

            if debug:
                logging.debug ('')
                logging.debug (f'save_data error: {str(e):s}')
            
            msg = 'Failed to open file [' + outpath + '] for write.'
            raise Exception (msg)    

        try:
            for data in response.iter_content(4096):
                
                len_data = len(data)            
        
                if (len_data < 1):
                    break

                fp.write (data)
        
            fp.close()

        except Exception as e:

            if debug:
                logging.debug ('')
                logging.debug (f'save_data error: {str(e):s}')
            
            self.msg = 'save_data error: ' + str(e)
            raise Exception (msg)    

        if debug:
            logging.debug ('')
            logging.debug (f'data written to file: {outpath:s}')
                
        #msg = 'Result downloaded to file [' + outpath + ']'
        #print (msg)
        return

#
#} end Archive.__get_moss_resultfile
#


    def __get_moss_status (self, statusurl, **kwargs):
#
#{ Archive.__get_moss_status
#
        status = ''
        msg = ''

        debug = 0
        
        if ('debug' in kwargs):
            debug = int(kwargs.get ('debug'))

        if debug:
            logging.debug ('')
            logging.debug ('Enter Koa.__get_moss_status:')

#
#    get status from statusurl
#
        response = None
        try:
            response = requests.get (statusurl, stream=True)
            
            if debug:
                logging.debug ('')
                logging.debug ('statusurl request sent')

        except Exception as e:
           
            msg = str(e)
	    
            if debug:
                logging.debug ('')
                logging.debug (f'exception: e= {str(e):s}')
            
            raise Exception (msg)    

        if debug:
            logging.debug ('')
            logging.debug ('statusurl response returned')
            logging.debug ('response= ')
            logging.debug (response)
       
        jsondata = None
        try:
            jsondata = response.json()
                    
        except Exception as e:
                
            if debug:
                logging.debug ('')
                logging.debug (f'JSON object parse error: {str(e):s}')
      
            status = 'error'
            msg = 'JSON parse error: ' + str(e)
                
            raise Exception (msg)    

            if debug:
                logging.debug ('')
                logging.debug (f'status= {status:s}')
                logging.debug (f'msg= {msg:s}')

        #status = jsondata['status']
        return (jsondata)
        
#
#} end Archive.__get_moss_status
#


    def print_data (self):
#
#{ Archive.print_date
#

        if self.debug:
            logging.debug ('')
            logging.debug ('Enter Koa.print_data:')

        try:
            self.tap.print_data ()
        except Exception as e:
                
            msg = 'Error print data: ' + str(e)
            print (msg)
        
        return
#
#} end Archive.print_date
#


    def download (self, metapath, format, outdir, **kwargs):
#
#{ Archive.download
#
    
        """
        'download' method allows download of FITS files shown in the 
        metadata file. The same method has the option of download the
        associated calibration files and level 1 files. 

        *** Requirement: 
            
            To download FITS files, the following two columns: 
            instrume and filehand must be included in the metadata file.

            To download associated calibrated files, the following three 
            columns: instrume, koaid, and filehand must be included in the 
            metadata file.


	Required input:
	-----
	metapath (string): a full path metadata table obtained from running
	          query methods    
       
	format (string):   metadata table's format: ipac, votable, csv, or tsv.
	
        outdir (string):   the directory for depositing the returned files      

        At least one of the following three optional parameters:
        lev1file, lev1file, and calibfile should be set to 1.:

	
        Optional input:
        ----------------
        cookiepath (string): cookie file path for downloading the proprietary 
                             KOA data;
        
        start_row (integer): default is start_row = 0;
	
        end_row (integer): default is end_row = nrows - 1 where nrows is the 
                           number of rows in the metadata file;

        lev0file (integer): 1/0; 
            1: download level0 files (i.e. raw data) in 'lev0' directory;
            0: do not download level0 files.
            default is 1.
        
        calibfile (integer): 1/0;
            1: download calibration files;
            0: do not download calibration files.
            default is 0.
         
            Note: The calibration files are saved in the directory determined 
            by paramter 'calibdir' -- either in the 'calib' sub-directory, or
            in the same directory as the raw data files.

        lev1file (integer): 1/0;
            1: download level1 files in 'lev1' directory;
            0: do not download level1 files.
            default is 0.
        
        calibdir (integer): 1/0; 
            1: put calibration files in their own directory named 'calib'; 
            0: put the calibration files in the 'lev0' directory with other 
                raw, science files.
            default is 1.
        """
       
        debug = 0
        debugfname = ''
        if ('debugfile' in kwargs):
            
            debug = 1
            debugfname = kwargs.get ('debugfile')

            if (len(debugfname) > 0):
      
                logging.basicConfig (filename=debugfname, \
                    level=logging.DEBUG)
    
                with open (debugfname, 'w') as fdebug:
                    pass

#
#    retrieve baseurl from conf class;
#
#    during dev or test, baseurl will be a keyword input
#
        self.baseurl = conf.server

        if ('server' in kwargs):
            self.baseurl = kwargs.get ('server')

        if debug:
            logging.debug ('')
            logging.debug (f'baseurl= {self.baseurl:s}')
            logging.debug ('Enter download:')
        
        if (len(metapath) == 0):
            print ('Failed to find required input parameter: metapath')
            return

        if (len(format) == 0):
            print ('Failed to find required input parameter: format')
            return

        if (len(outdir) == 0):
            print ('Failed to find required input parameter: outdir')
            return
 

        if debug:
            logging.debug ('')
            logging.debug (f'metapath= {metapath:s}')
            logging.debug (f'format= {format:s}')
            logging.debug (f'outdir= {outdir:s}')

        
        cookiepath = ''
        cookiejar = None
        
        if ('cookiepath' in kwargs): 
            cookiepath = kwargs.get('cookiepath')

        if debug:
            logging.debug ('')
            logging.debug (f'cookiepath= {cookiepath:s}')

        if (len(cookiepath) > 0):
   
            cookiejar = http.cookiejar.MozillaCookieJar (cookiepath)

            try: 
                cookiejar.load (ignore_discard=True, ignore_expires=True)
    
                if debug:
                    logging.debug (\
                        f'cookie loaded from file: {cookiepath:s}')
        
                for cookie in cookiejar:
                    
                    if debug:
                        logging.debug ('')
                        logging.debug ('cookie=')
                        logging.debug (cookie)
                        logging.debug (f'cookie.name= {cookie.name:s}')
                        logging.debug (f'cookie.value= {cookie.value:s}')
                        logging.debug (f'cookie.domain= {cookie.domain:s}')

            except Exception as e:
                if debug:
                    logging.debug ('')
                    logging.debug (f'loadCookie exception: {str(e):s}')
                pass

#        endif (cookiepath)

        fmt_astropy = format
        if (format == 'tsv'):
            fmt_astropy = 'ascii.tab'
        if (format == 'csv'):
            fmt_astropy = 'ascii.csv'
        if (format == 'ipac'):
            fmt_astropy = 'ascii.ipac'

#
#    read metadata to astropy table
#
        astropytbl = None
        try:
            astropytbl = Table.read (metapath, format=fmt_astropy)
        
        except Exception as e:
            msg = 'Failed to read metadata table to astropy table:' + \
                str(e) 
            print (msg)
            return
            #sys.exit()

        len_tbl = len(astropytbl)

        if debug:
            logging.debug ('')
            logging.debug ('astropytbl read')
            logging.debug (f'len_tbl= {len_tbl:d}')

        
        colnames = astropytbl.colnames

        if debug:
            logging.debug ('')
            logging.debug ('colnames:')
            logging.debug (colnames)
  
        len_col = len(colnames)

        if debug:
            logging.debug ('')
            logging.debug (f'len_col= {len_col:d}')

 
        ind_instrume = -1
        ind_koaid = -1
        ind_filehand = -1
        for i in range (len_col):

            if (colnames[i].lower() == 'instrume'):
                ind_instrume = i

            if (ind_instrume == -1): 
                if (colnames[i].lower() == 'instrument'):
                    ind_instrume = i
            
            if (colnames[i].lower() == 'koaid'):
                ind_koaid = i

            if (colnames[i].lower() == 'filehand'):
                ind_filehand = i
             
        if debug:
            logging.debug ('')
            logging.debug (f'ind_instrume= {ind_instrume:d}')
            logging.debug (f'ind_koaid= {ind_koaid:d}')
            logging.debug (f'ind_filehand= {ind_filehand:d}')
      
        if (ind_instrume == -1):
            print ('Column [instrume] is required in the metadata file for downloading data.')
            return
            #sys.exit()
        
        if (ind_koaid == -1):
            print ('Column [koaid] is required in the metadata file for downloading data.')
            return
            #sys.exit()
        
        if (ind_filehand == -1):
            print ('Column [filehand] is required in the metadata file for downloading data.')
            return
            #sys.exit()
    
        if (len_tbl == 0):
            print ('There is no data in the metadata table.')
            return
            #sys.exit()
        
        lev0file = 1 
        if ('lev0file' in kwargs): 
            lev0file = kwargs.get('lev0file')
         
        calibfile = 0 
        if ('calibfile' in kwargs): 
            calibfile = kwargs.get('calibfile')
         
        lev1file = 0 
        if ('lev1file' in kwargs): 
            lev1file = kwargs.get('lev1file')
         
        calibdir = 1 
        if ('calibdir' in kwargs): 
            calibdir = kwargs.get('calibdir')
         
        if debug:
            logging.debug ('')
            logging.debug (f'lev0file= {lev0file:d}')
            logging.debug (f'calibfile= {calibfile:d}')
            logging.debug (f'lev1file= {lev1file:d}')
            logging.debug (f'calibdir= {calibdir:d}')

        """
        if ((lev0file == 0) and \
            (lev1file == 0) and \
            (calibfile == 0)):
            
            print ('Please choose which types of data you wish to download: lev0file, lev1file, and calibfile, more than one choice is allowed.')
            return
        """

        srow = 0;
        erow = len_tbl - 1

        if ('start_row' in kwargs): 
            srow = kwargs.get('start_row')

        if debug:
            logging.debug ('')
            logging.debug (f'srow= {srow:d}')
     
        if ('end_row' in kwargs): 
            erow = kwargs.get('end_row')
        
        if debug:
            logging.debug ('')
            logging.debug (f'erow= {erow:d}')
     
        if (srow < 0):
            srow = 0 
        if (erow > len_tbl - 1):
            erow = len_tbl - 1 
 
        if debug:
            logging.debug ('')
            logging.debug (f'srow= {srow:d}')
            logging.debug (f'erow= {erow:d}')
     

#
#    create outdir for lev0, lev1, calib data if it doesn't exist
#
#    decimal mode work for both python2.7 and python3;
#
#    0755 also works for python 2.7 but not python3
#  
#    convert octal 0775 to decimal: 493 
#
        d1 = int ('0775', 8)

        if debug:
            logging.debug ('')
            logging.debug (f'd1= {d1:d}')
#
#    lev0 subdir 
#
        outdir_lev0 = outdir
        if (lev0file == 1):
            outdir_lev0 = outdir + '/lev0'
            
        try:
            os.makedirs (outdir_lev0, mode=d1, exist_ok=True) 

        except Exception as e:
            
            msg = f'Failed to create {outdir:s}: {str(e):s}'
            print (msg)
            return
            #sys.exit()
   
        if debug:
            logging.debug ('')
            logging.debug ('returned os.makedirs for lev0 data subdir') 

#
#    lev1 subdir 
#
        outdir_lev1 = outdir
        if (lev1file == 1):
            outdir_lev1 = outdir + '/lev1'
            
        try:
            os.makedirs (outdir_lev1, mode=d1, exist_ok=True) 

        except Exception as e:
            
            msg = f'Failed to create {outdir:s}: {str(e):s}'
            print (msg)
            return

        if debug:
            logging.debug ('')
            logging.debug ('returned os.makedirs for lev1 data subdir') 

#
#    calib subdir 
#
        outdir_calib = outdir
        if (calibfile == 1):
            
            if (lev0file == 0):
                outdir_calib = outdir + '/calib'
            else:
                if (calibdir == 1):
                    outdir_calib = outdir + '/calib'
                else:
                    outdir_calib = outdir + '/lev0'

        try:
            os.makedirs (outdir_calib, mode=d1, exist_ok=True) 

        except Exception as e:
            
            msg = f'Failed to create {outdir:s}: {str(e):s}'
            print (msg)
            return

        if debug:
            logging.debug ('')
            logging.debug ('returned os.makedirs for calib data subdir') 

        if debug:
            logging.debug ('')
            logging.debug (f'outdir_lev0= {outdir_lev0:s}')
            logging.debug (f'outdir_lev1= {outdir_lev1:s}')
            logging.debug (f'outdir_calib= {outdir_calib:s}')

#
#    urls for nph-getKoa, and nph-getCaliblist
#
        self.getkoa_url = self.baseurl + \
            'cgi-bin/getKOA/nph-getKOA?return_mode=json&'
        self.caliblist_url = self.baseurl + \
            'cgi-bin/KoaAPI/nph-getCaliblist?'
        self.lev1list_url = self.baseurl + 'cgi-bin/KoaAPI/nph-getL1list?'

        if debug:
            logging.debug ('')
            logging.debug (f'getkoa_url= {self.getkoa_url:s}')
            logging.debug (f'caliblist_url= {self.caliblist_url:s}')


        instrument = '' 
        koaid = ''
        filehand = ''
        ndnloaded_lev0 = 0
        
        nlev1list = 0
        ndnloaded_lev1 = 0
        
        ncaliblist = 0
        ndnloaded_calib = 0
      
        nfile = erow - srow + 1   
        
        print (f'Start downloading {nfile:d} koaid data you requested;')
        print (f'please check your outdir: {outdir:s} for  progress ....')
 
        for l in range (srow, erow+1):
        #
        #{ for loop for download all files (lev0, lev1, calib)
        #
            if debug:
                logging.debug ('')
                logging.debug (f'l= {l:d}')
                logging.debug ('')
                logging.debug ('astropytbl[l]= ')
                logging.debug (astropytbl[l])
                logging.debug ('instrument= ')
                logging.debug (astropytbl[l][ind_instrume])

            instrument = astropytbl[l][ind_instrume]
            koaid = astropytbl[l][ind_koaid]
            filehand = astropytbl[l][ind_filehand]
	    
            if debug:
                logging.debug ('')
                logging.debug ('type(instrument)= ')
                logging.debug (type(instrument))
                logging.debug (type(instrument) is bytes)
            
            if (type (instrument) is bytes):
                
                if debug:
                    logging.debug ('')
                    logging.debug ('bytes: decode')

                instrument = instrument.decode("utf-8")
                koaid = koaid.decode("utf-8")
                filehand = filehand.decode("utf-8")
           
            ind = -1
            ind = instrument.find ('HIRES')
            if (ind >= 0):
                instrument = 'HIRES'
            
            ind = -1
            ind = instrument.find ('LRIS')
            if (ind >= 0):
                instrument = 'LRIS'
 
            ind = -1
            ind = instrument.find ('NIRS')
            if (ind >= 0):
                instrument = 'NIRSPEC'
  
            if debug:
                logging.debug ('')
                logging.debug (f'l= {l:d} koaid= {koaid:s}')
                logging.debug (f'filehand= {filehand:s}')
                logging.debug (f'instrument= {instrument:s}')

            #
            #   get lev0 files
            #
            if (lev0file == 1):
            
                url = self.getkoa_url + 'filehand=' + filehand
                filepath = outdir_lev0 + '/' + koaid
                
                if debug:
                    logging.debug ('')
                    logging.debug (f'filepath= {filepath:s}')
                    logging.debug (f'url= {url:s}')

                #
                #    if file doesn't exist: download
                #
                isExist = os.path.exists (filepath)
	    
                if (not isExist):

                    try:
                        self.__submit_request (url, filepath, cookiejar)
                        ndnloaded_lev0 = ndnloaded_lev0 + 1

                        msg =  'Returned file written to: ' + filepath   
           
                        if debug:
                            logging.debug ('')
                            logging.debug ('returned __submit_request')
                            logging.debug (f'self.msg= {msg:s}')
            
                    except Exception as e:
                        print (f'File [{koaid:s}] download error: {str(e):s}')

                if debug:
                    logging.debug ('')
                    logging.debug (f'ndnloaded_lev0= {ndnloaded_lev0:d}')
            

            if (lev1file == 1):
            #
            # { if leve1file == 1   
            #
                if ((instrument.lower() != "nirc2") and \
                    (instrument.lower() != "osiris") and \
                    (instrument.lower() != "lws") and \
                    (instrument.lower() != "hires") and \
                    (instrument.lower() != "nirspec")):
               
                    if (l == srow):
                        print (f'Instrument [{instrument:s}] does not have level1 data.')
                else:
                #
                # {   this instrument might have lev1 data
                #
                    #
                    # { get lev1 list 
                    #
                    if debug:
                        logging.debug ('')
                        logging.debug ('lev1file=1: downloading lev1list')
	  
                    koaid_base = '' 
                    ind = -1
                    ind = koaid.rfind ('.')
                    if (ind > 0):
                        koaid_base = koaid[0:ind]
                    else:
                        koaid_base = koaid

                    if debug:
                        logging.debug ('')
                        logging.debug (f'koaid_base= {koaid_base:s}')
	    
                    lev1list = outdir_lev1 + '/' + koaid_base + '.lev1list.json'
                
                    if debug:
                        logging.debug ('')
                        logging.debug (f'lev1list= {lev1list:s}')

                    isExist = os.path.exists (lev1list)
	    
                    if (not isExist):

                        if debug:
                            logging.debug ('')
                            logging.debug ('downloading lev1list')
	    
                        url = self.lev1list_url \
                            + 'instrument=' + instrument \
                            + '&koaid=' + koaid \
                            + '&filehand=' + filehand


                        if debug:
                            logging.debug ('')
                            logging.debug (f'lev1list url= {url:s}')

                        try:
                            #self.__submit_request (url, lev1list, cookiejar, \
                            #    debug=1)
                            self.__submit_request (url, lev1list, cookiejar)
                            
                            nlev1list = nlev1list + 1

                            msg =  'Returned file written to: ' + lev1list 
           
                            if debug:
                                logging.debug ('')
                                logging.debug ('returned __submit_request')
                                logging.debug (f'msg= {msg:s}')
                                logging.debug (f'nlev1list= {nlev1list:d}')
            
                        except Exception as e:
                        
                            msg = 'Failed to get level 1 file list ' \
                                + 'for koaid: ' + koaid
                        
                            print (f'{msg:s}')
                            print (str(e))
                        
                    #
                    # } end get lev1 list 
                    #
                    #
                    #    check again after lev1list is successfully downloaded, 
                    #     
                
                    nlev1file = 0
                
                    isExist = os.path.exists (lev1list)
                
                    if (not isExist):
                        msg = 'Failed to get level 1 data list ' \
                            + 'for koaid: ' + koaid
                        
                        print (f'{msg:s}')
                
                    else:
                    #     
                    #  extract koaid and nlev1file from json strcuture
                    #     
                
                        jsonData = None
                        koaid = ''
                        try:
                            with open (lev1list) as fp:
	    
                                jsonData = json.load (fp) 
                                koaid = jsonData["input"]["koaid"]
                                nlev1file = int(jsonData["result"]["nlev1file"])
                            fp.close() 
                            
                        except Exception as e:
        
                            if debug:
                                logging.debug ('')
                                logging.debug ( \
                                    f'lev1list: {lev1list:s} load error')

                            msg = 'Failed to read ' + lev1list	
                            print (f'{msg:s}')
                            fp.close() 

                        if debug:
                            logging.debug ('')
                            logging.debug (f'koaid= {koaid:s}')
                            logging.debug (f'nlev1file= {nlev1file:d}')
  
                    if (nlev1file == 0):
                    
                        if debug:
                            logging.debug ('')
                            logging.debug (f'got here:')
                            logging.debug (f'nlev1file= {nlev1file:d}')
  
                        msg = 'No level 1 data found for koaid: [' \
                            + koaid + ']'
                    
                        print (f'{msg:s}')
                
                    else:   
                    #
                    # { nlev1file > 0: download lev1file
                    #
                        if debug:
                            logging.debug ('')
                            logging.debug ('list exist: downloading lev1files')

                    
                        #if ((instrument.lower() != "hires") or \
                        #    (instrument.lower() != "nirspec")):
                        #    print ('')
                        #    print ( \
                        #        f'Downloading [{koaid:s}] level 1 files ....')
                        
                        #print ('')
                        #print (f'Downloading [{koaid:s}] level 1 files ....')
                        
                        try:
                            nlev1 = self.__download_lev1files (jsonData, \
                                cookiejar, outdir_lev1)
                        
                            #nlev1 = self.__download_lev1files (jsonData, \
                            #    cookiejar, outdir_lev1, debug=1)
                    
                            if debug:
                                logging.debug ('')
                                logging.debug (f'returned __download_lev1files')
                                logging.debug (f'nlev1= {nlev1:d}')
                        
                            ndnloaded_lev1 = ndnloaded_lev1 + nlev1
                    
                            if debug:
                                logging.debug ('')
                                logging.debug ( \
                                    f'ndnloaded_lev1= {ndnloaded_lev1:d}')
                           
                            msg = str(nlev1) + ' level1 files downloaded ' \
                                + 'for koaid: [' + koaid + ']'

                            if debug:
                                logging.debug ('')
                                logging.debug (f'msg= {msg:s}')
                           
                            #print (f'{msg:s}')
         
                            if debug:
                                logging.debug ('')
                                logging.debug ('returned __download_lev1files')
                                logging.debug (f'{nlev1:d} downloaded')
                                logging.debug ( \
                                    f'ndnloaded_lev1= {ndnloaded_lev1:d}')
                
                        except Exception as e:
                
                            msg = 'Error downloading files in lev1list [' + \
                                lev1list + ']: ' +  str(e)
                            print (f'{msg:s}')
                        
                            if debug:
                                logging.debug ('')
                                logging.debug (f'errmsg= {msg:s}')

                    #
                    # } download lev1 files
                    #     
                #
                # } end lev1 files dnload for the instrument
                #     
            # 
            #} endif (lev1file == 1):
            #
                        
                if debug:
                    logging.debug ('')
                    logging.debug ('done lev1 dnload')
                    logging.debug (f'ndnloaded= {ndnloaded_lev1:d}')
                

            if (calibfile == 1):
            #
            # {   if calibfile == 1: download calibfile
            #
    
                if debug:
                    logging.debug ('')
                    logging.debug ('calibfile=1: downloading calibfiles')
	    
                koaid_base = '' 
                ind = -1
                ind = koaid.rfind ('.')
                if (ind > 0):
                    koaid_base = koaid[0:ind]
                else:
                    koaid_base = koaid

                if debug:
                    logging.debug ('')
                    logging.debug (f'koaid_base= {koaid_base:s}')
	    
                caliblist = outdir_calib + '/' + koaid_base + '.caliblist.json'
                caliblist_ipac = outdir_calib + '/' + koaid_base + '.caliblist.tbl'
                
                if debug:
                    logging.debug ('')
                    logging.debug (f'caliblist= {caliblist:s}')
                    logging.debug (f'caliblist_ipac= {caliblist_ipac:s}')

                #
                #    download caliblist (json)
                #
                isExist = os.path.exists (caliblist)
	    
                if (not isExist):

                    if debug:
                        logging.debug ('')
                        logging.debug ('downloading caliblist')
	    
                    url = self.caliblist_url \
                        + 'instrument=' + instrument \
                        + '&koaid=' + koaid

                    if debug:
                        logging.debug ('')
                        logging.debug (f'caliblist url= {url:s}')

                    try:
                        self.__submit_request (url, caliblist, cookiejar)
                        ncaliblist = ncaliblist + 1

                        msg =  'Returned file written to: ' + caliblist   
           
                        if debug:
                            logging.debug ('')
                            logging.debug ('returned __submit_request')
                            logging.debug (f'msg= {msg:s}')
            
                    except Exception as e:
                        #print (f'File [{caliblist:s}] download: {str(e):s}')
                        #msg = 'Error downloading caliblist [' + \
                        #    caliblist + ']:' + str(e)
                        
                        msg = 'No associated calibration list for ' + \
                            koaid
                        print (f'{msg:s}')
                        continue 
                         

                #
                #    download caliblist_ipac
                #
                isExist = os.path.exists (caliblist_ipac)
	    
                if (not isExist):

                    if debug:
                        logging.debug ('')
                        logging.debug ('downloading caliblist_ipac')
	    
                    url = self.caliblist_url \
                        + 'instrument=' + instrument \
                        + '&koaid=' + koaid + '&format=ipac'

                    if debug:
                        logging.debug ('')
                        logging.debug (f'caliblist_ipac url= {url:s}')

                    try:
                        self.__submit_request (url, caliblist_ipac, cookiejar)
                        msg =  'Returned file written to: ' + caliblist_ipac   
           
                        if debug:
                            logging.debug ('')
                            logging.debug ('returned __submit_request')
                            logging.debug (f'msg= {msg:s}')
            
                    except Exception as e:
                        #print (f'File [{caliblist:s}] download: {str(e):s}')
                        #msg = 'Error downloading caliblist_ipac [' + \
                        #    caliblist_ipac + ']:' + str(e)
                        
                        msg = 'No associated calibration list for ' + \
                            koaid
                        print (f'{msg:s}')
                        continue 
                         

#
#    check again after caliblist is successfully downloaded, if caliblist 
#    exists: download calibfiles
#     
                isExist = os.path.exists (caliblist)
                                  
                if (isExist):
                #
                #{ download_calibfiles:
                #

                    if debug:
                        logging.debug ('')
                        logging.debug ('list exist: downloading calibfiles')
	   
                    #if ((instrument.lower() != "hires") or \
                    #    (instrument.lower() != "nirspec")):
                    #    print ('')
                    #    print ( \
                    #        f'Downloading [{koaid:s}] calibration files ....')
                    
                    #print ('')
                    #print (f'Downloading [{koaid:s}] calibration files ....')
                        
                    try:
                        #ncalibs = self.__download_calibfiles ( \
                        #    caliblist, cookiejar, outdir_calib)
                        
                        ncalibs = self.__download_calibfiles ( \
                            caliblist, cookiejar, outdir_calib, deubg=1)
                        ndnloaded_calib = ndnloaded_calib + ncalibs
                
                        if debug:
                            logging.debug ('')
                            logging.debug ('returned __download_calibfiles')
                            logging.debug (f'{ncalibs:d} downloaded')

                        msg = str(ncalibs) + ' calibration files downloaded ' \
                            + 'for koaid: [' + koaid + ']'
                        #print (msg)

                    except Exception as e:
                
                        msg = 'Error downloading files in caliblist [' + \
                            filepath + ']: ' +  str(e)
                        
                        if debug:
                            logging.debug ('')
                            logging.debug (f'errmsg= {msg:s}')
                
                #
                #} endif (download_calibfiles):
                #
            # 
            #} endif (calibfile == 1):
            #
            
        #
        #}        endfor l in range (srow, erow+1)
        #

        if debug:
            logging.debug ('')
            logging.debug (f'{len_tbl:d} files in the table;')
            logging.debug (f'{ndnloaded_lev0:d} lev0 files downloaded.')
            logging.debug (f'{nlev1list:d} lev1list downloaded.')
            logging.debug (\
                f'{ndnloaded_lev1:d} lev1files downloaded.')
            logging.debug (f'{ncaliblist:d} calibration list downloaded.')
            logging.debug (\
                f'{ndnloaded_calib:d} calibration files downloaded.')
        #
        #    print out total count of downloaded files
        #
        print ('')
        if (lev0file == 1):
            print (f'A total of {ndnloaded_lev0:d} new lev0 FITS files downloaded.')
 
        if (lev1file == 1):
            
            if ((instrument.lower() == "nirc2") or \
                (instrument.lower() == "osiris") or \
                (instrument.lower() == "lws") or \
                (instrument.lower() == "hires") or \
                (instrument.lower() == "nirspec")):
               
                print (f'{nlev1list:d} new lev1 list downloaded.')
                print (f'{ndnloaded_lev1:d} new lev1 files downloaded.')
        
        if (calibfile == 1):
            print (f'{ncaliblist:d} new calibration list downloaded.')
            print (f'{ndnloaded_calib:d} new calibration FITS files downloaded.')
        return
#
#} end Archive.download
#
    

    def __download_lev1files (self, jsonData, cookiejar, outdir_lev1, \
        **kwargs):
#
#{ Archive.__download_lev1files
#
        debug = 0
        
        if ('debug' in kwargs):
            debugstr = kwargs.get ('debug')
            debug = int(debugstr)
   
        if debug:
            logging.debug ('')
            logging.debug (f'Enter __download_lev1files:')
            logging.debug (f'outdir_lev1= {outdir_lev1:s}')

#
#    read input lev1list JSON file
#
        instrument = jsonData["input"]["instrument"]
        koaid = jsonData["input"]["koaid"]
        filehand = jsonData["input"]["filehand"]
        nlev1file = int(jsonData["result"]["nlev1file"])
        lev1subdir_prefix = jsonData["result"]["lev1subdir_prefix"]
                
        if debug:
            logging.debug ('')
            logging.debug (f'lev1subdir_prefix= {lev1subdir_prefix:s}')
            logging.debug (f'instrument= {instrument:s}')
            logging.debug (f'koaid= {koaid:s}')
            logging.debug (f'filehand= {filehand:s}')
            logging.debug (f'nlev1file= {nlev1file:d}')
        
        data = ''
        if ((instrument.lower() == 'nirc2') or \
            (instrument.lower() == 'osiris') or \
            (instrument.lower() == 'lws')):
                
            data = jsonData["result"]["lev1file"]
                    
        elif ((instrument.lower() == 'hires') or \
            (instrument.lower() == 'nirspec')):

            data = jsonData["result"]["data"]
                    
        if debug:
            logging.debug ('')
            logging.debug (f'data:')
            logging.debug (data)


#
#    retrieve koaid from lev1list json structure and download files
#
        if debug:
            logging.debug ('Start downloading from lev1list:')
        

        filehand_lev1 = ''
        lev1file = ''
        nrec = 0 
        nsubdir = 0 
        
        nrec_total = 0

        if ((instrument.lower() == 'nirc2') or \
            (instrument.lower() == 'osiris') or \
            (instrument.lower() == 'lws')):
        #
        # { if n2, os, lw
        #
            if debug:
                logging.debug ('here0')
            
            if debug:
                logging.debug (f'nlev1file= {nlev1file:d}')

            for ind in range (nlev1file):

                if debug:
                    logging.debug (f'downloadlev1files: ind= {ind:d}')

                lev1file = data[ind]
                filehand_lev1 = lev1subdir_prefix + '/' + lev1file 
  
                if debug:
                    logging.debug (f'lev1file= {lev1file:s}')
                    logging.debug (f'filehand_lev1= {filehand_lev1:s}')

                filepath = outdir_lev1 + '/' + lev1file 
            
                if debug:
                    logging.debug (f'filepath= {filepath:s}')

                
#
#    if file exists, skip
#
                isExist = os.path.exists (filepath)
	    
                if (isExist):
                    if debug:
                        logging.debug ('')
                        logging.debug (f'isExist: {isExist:d}: skip')
                     
                    continue
              
                url = self.baseurl + 'cgi-bin/KoaAPI/nph-dnloadL1data?' \
                    + 'instrument=' + instrument + '&koaid=' + koaid \
                    + '&filehand=' + filehand_lev1
                 
                if debug:
                    logging.debug (f'url= {url:s}')

                try:
                    self.__submit_request (url, filepath, cookiejar)
                    nrec_total = nrec_total + 1
                
                    msg = 'lev1 file [' + filepath + '] downloaded.'

                    if debug:
                        logging.debug ('')
                        logging.debug ('returned __submit_request')
                        logging.debug (f'msg: {msg:s}')
                        logging.debug (f'nrec_total= {nrec_total:d}')
            
            
                except Exception as e:
                
                    print (f'lev1 file download error: {str(e):s}')

            if debug:
                logging.debug ('')
                logging.debug (f'instrument: {instrument:s}')
                logging.debug (f'{nrec_total:d} files downloaded.')
            
        #
        # } end if (n2,lws,os)
        #
        elif ((instrument.lower() == 'hires') or \
            (instrument.lower() == 'nirspec')):

        #
        # { if (ns,hi)
        #
            nsubdir = len (data)

            if debug:
                logging.debug ('')
                logging.debug (f'nsubdir= {nsubdir:d}')
                logging.debug (f'lev1subdir_prefix= {lev1subdir_prefix:s}')
            
            lev1filepath = ''
            subdir = ''
            lev1files = ''
            filehand_lev1 = ''
            url = ''
            d1 = int('0755', 8)
            nrec = 0
            for l in range (nsubdir):
            #for l in range (0, 1):

                subdir = data[l]["subdir"] 
                lev1files = data[l]["lev1files"] 
                nrec = len (lev1files) 
              
                if debug:
                    logging.debug ('')
                    logging.debug (f'l= {l:d} subdir= {subdir:s}')
                    logging.debug (f'nrec= {nrec:d}')
                    #logging.debug (f'lev1files=')
                    #logging.debug (lev1files)
        
        
                for i in range (nrec):
                #for i in range (0, 1):


                    if debug:
                        logging.debug (f'downloadlev1files: i= {i:d}')

                    lev1file = lev1files[i]
                    
                    if debug:
                        logging.debug ('')
                        logging.debug (f'lev1file= {lev1file:s}')
                    
                    filehand_lev1 = \
                        lev1subdir_prefix + '/' + subdir + '/' + lev1file 
                    
                    if debug:
                        logging.debug ('')
                        logging.debug (f'filehand_lev1= {filehand_lev1:s}')
                    
                    lev1filepath = outdir_lev1 + '/' + subdir
                    
                    if debug:
                        logging.debug ('')
                        logging.debug (f'lev1filepath= {lev1filepath:s}')
                    
                    os.makedirs (lev1filepath, mode=d1, exist_ok=True) 

                    filepath = lev1filepath + '/'+ lev1file 
            
                    if debug:
                        logging.debug ('')
                        logging.debug (f'filepath= {filepath:s}')

                    url = self.baseurl + 'cgi-bin/KoaAPI/nph-dnloadL1data?' \
                        + 'instrument=' + instrument + '&koaid=' + koaid \
                        + '&filehand=' + filehand_lev1
                    
                    if debug:
                        logging.debug ('')
                        logging.debug (f'url= {url:s}')
                     
#
#    if file exists, skip
#
                    isExist = os.path.exists (filepath)
	    
                    if (isExist):
                        if debug:
                            logging.debug ('')
                            logging.debug (f'isExist: {isExist:d}: skip')
                     
                        continue

                    try:
                        self.__submit_request (url, filepath, cookiejar)
                
                        msg = 'lev1 file [' + filepath + '] downloaded.'
                        nrec_total = nrec_total + 1

                        if debug:
                            logging.debug ('')
                            logging.debug ('returned __submit_request')
                            logging.debug (f'msg: {msg:s}')
                            logging.debug (f'nrec_total= {nrec_total:d}')
            
                    except Exception as e:
                
                        print (f'error downloading lev1 file {lev1file:s}: {str(e):s}')

            if debug:
                logging.debug ('')
                logging.debug (f'instrument: {instrument:s}')
                logging.debug (f'{nrec_total:d} files downloaded.')
        
        #
        # } end elif ns, hi
        #
        if debug:
            logging.debug ('')
            logging.debug (f'{nrec_total:d} files downloaded.')

        return (nrec_total)
#
#} end  Archive.__download_lev1files
#


    def __download_calibfiles (self, listpath, cookiejar, outdir_calib, \
        **kwargs):
#
#{ Archive.__download_calibfiles
#
        debug = 0
        
        if ('debug' in kwargs):
            debugstr = kwargs.get ('debug')
            debug = int(debugstr)
    
    
        if debug:
            logging.debug ('')
            logging.debug (f'Enter __download_calibfiles: {listpath:s}')

#
#    read input caliblist JSON file
#
        nrec = 0
        data = ''
        try:
            with open (listpath) as fp:
	    
                jsonData = json.load (fp) 
                data = jsonData["table"]

            fp.close() 

        except Exception as e:
        
            if debug:
                logging.debug ('')
                logging.debug (f'caliblist: {caliblist:s} load error')

            errmsg = 'Failed to read ' + listpath	
	
            fp.close() 
            
            raise Exception (errmsg)

            return

        nrec = len(data)
    
        if debug:
            logging.debug ('')
            logging.debug (f'downloadCalibfiles: nrec= {nrec:d}')

        if (nrec == 0):

            status = 'error'	
            errmsg = 'No data found in the caliblist: ' + listpath
	    
            raise Exception (errmsg)


#
#    retrieve koaid from caliblist json structure and download files
#
        if debug:
            logging.debug ('')
            logging.debug (f'got here: nrec= {nrec:d}')

        ndnloaded = 0
        for ind in range (nrec):

            if debug:
                logging.debug (f'downloadCalibfiles: ind= {ind:d}')

            koaid = data[ind]['koaid']
            instrument = data[ind]['instrument']
            filehand = data[ind]['filehand']
            
            if debug:
                logging.debug (f'instrument= {instrument:s}')
                logging.debug (f'koaid= {koaid:s}')
                logging.debug (f'filehand= {filehand:s}')

#
#   get lev0 files
#
            url = self.getkoa_url + 'filehand=' + filehand
                
            filepath = outdir_calib + '/' + koaid
                
            if debug:
                logging.debug ('')
                logging.debug (f'filepath= {filepath:s}')
                logging.debug (f'url= {url:s}')

#
#    if file exists, skip
#
            isExist = os.path.exists (filepath)
	    
            if (isExist):
                if debug:
                    logging.debug ('')
                    logging.debug (f'isExist: {isExist:d}: skip')
                     
                continue

            try:
                self.__submit_request (url, filepath, cookiejar)
                ndnloaded = ndnloaded + 1
                
                msg = 'calib file [' + filepath + '] downloaded.'

                if debug:
                    logging.debug ('')
                    logging.debug ('returned __submit_request')
                    logging.debug (f'msg: {msg:s}')
            
            except Exception as e:
                print (f'calib file download error: {str(e):s}')

        if debug:
            logging.debug ('')
            logging.debug (f'nfnlosfrf= {ndnloaded:d}')

        return (ndnloaded)
#
#} end  Archive.__download_calibfiles
#
    

    def __submit_request(self, url, filepath, cookiejar, **kwargs):
#
#{ Archive.__submit_request
#
        debug = 0
        
        if ('debug' in kwargs):
            debugstr = kwargs.get ('debug')
            debug = int(debugstr)

        if debug:
            logging.debug ('')
            logging.debug ('Enter database.__submit_request:')
            logging.debug (f'url= {url:s}')
            logging.debug (f'filepath= {filepath:s}')
       
            if not (cookiejar is None):  
            
                for cookie in cookiejar:
                    
                    if debug:
                        logging.debug ('')
                        logging.debug ('cookie saved:')
                        logging.debug (f'cookie.name= {cookie.name:s}')
                        logging.debug (f'cookie.value= {cookie.value:s}')
                        logging.debug (f'cookie.domain= {cookie.domain:s}')
            
        try:
            self.response =  requests.get (url, stream=True, cookies=cookiejar)

            #self.response =  requests.get (url, cookies=cookiejar, \
            #    stream=True)

            if debug:
                logging.debug ('')
                logging.debug ('-------------------------------------')
                logging.debug ('URL:' + url)
                logging.debug ('Cookiejar type:')
                logging.debug (type(cookiejar))
                
                logging.debug ('request sent')
                logging.debug ('done')
                logging.debug ('')
        
        
        except Exception as e:
            
            if debug:
                logging.debug ('')
                logging.debug (f'exception: {str(e):s}')

            msg = 'Failed to submit the request: ' + str(e)
	    
            raise Exception (msg)
            return
                       
        if debug:
            logging.debug ('')
            logging.debug ('status_code:')
            logging.debug (self.response.status_code)
      
      
        if (self.response.status_code == 200):
            msg = ''
        else:
            msg = 'Failed to submit the request'
	    
            raise Exception (msg)
            return
                       
        if debug:
            logging.debug ('')
            logging.debug ('headers: ')
            logging.debug (self.response.headers)
      
        content_type = ''
        try:
            content_type = self.response.headers['Content-type']
        except Exception as e:

            if debug:
                logging.debug ('')
                logging.debug (f'exception extract content-type: {str(e):s}')

        if debug:
            logging.debug ('')
            logging.debug (f'content_type= {content_type:s}')
            

        if (content_type == 'application/json'):
            
            if debug:
                logging.debug ('')
                logging.debug (\
                    'return is a json structure: might be error message')
            
            jsondata = json.loads (self.response.text)
          
            if debug:
                logging.debug ('')
                logging.debug ('jsondata:')
                logging.debug (jsondata)

 
            status = ''
            try: 
                status = jsondata['status']
                
                if debug:
                    logging.debug ('')
                    logging.debug (f'status= {status:s}')

            except Exception as e:

                if debug:
                    logging.debug ('')
                    logging.debug (f'get status exception: e= {str(e):s}')

            msg = '' 
            try: 
                msg = jsondata['msg']
                
                if debug:
                    logging.debug ('')
                    logging.debug (f'msg= {msg:s}')

            except Exception as e:

                if debug:
                    logging.debug ('')
                    logging.debug (f'extract msg exception: e= {str(e):s}')

            errmsg = ''        
            try: 
                errmsg = jsondata['error']
                
                if debug:
                    logging.debug ('')
                    logging.debug (f'errmsg= {errmsg:s}')

                if (len(errmsg) > 0):
                    status = 'error'
                    msg = errmsg

            except Exception as e:

                if debug:
                    logging.debug ('')
                    logging.debug (f'get error exception: e= {str(e):s}')


            if debug:
                logging.debug ('')
                logging.debug (f'status= {status:s}')
                logging.debug (f'msg= {msg:s}')


            if (status == 'error'):
                raise Exception (msg)
                return

#
#    save to filepath
#
        if debug:
            logging.debug ('')
            logging.debug ('save_to_file:')
       
        try:
            with open (filepath, 'wb') as fd:

                for chunk in self.response.iter_content (chunk_size=1024):
                    fd.write (chunk)
            
            msg =  'Returned file written to: ' + filepath   
#            print (self.msg)
            
            if debug:
                logging.debug ('')
                logging.debug (msg)
	
        except Exception as e:

            if debug:
                logging.debug ('')
                logging.debug (f'exception: {str(e):s}')

            msg = 'Failed to save returned data to file: %s' % filepath
            
            raise Exception (msg)
            return

        return
#
#} end Archive.__submit_request
#
                       

    def __make_query (self, url, **kwargs):
#
#{ Archive.__make_query
#
        debug = 0
        
        if ('debug' in kwargs):
            debugstr = kwargs.get ('debug')
            debug = int(debugstr)
    
       
        if debug:
            logging.debug ('')
            logging.debug ('Enter __make_query:')
            logging.debug (f'url= {url:s}')

        response = None
        try:
            response = requests.get (url, stream=True)

            if debug:
                logging.debug ('')
                logging.debug ('request sent')

        except Exception as e:
           
            msg = 'Error: ' + str(e)

            if debug:
                logging.debug ('')
                logging.debug (f'exception: e= {str(e):s}')
            
            raise Exception (msg)


        content_type = response.headers['content-type']

        if debug:
            logging.debug ('')
            logging.debug (f'content_type= {content_type:s}')
       
        if (content_type == 'application/json'):
                
            if debug:
                logging.debug ('')
                logging.debug (f'response.text: {response.text:s}')

#
#    error message
#
            try:
                jsondata = json.loads (response.text)
                 
                if debug:
                    logging.debug ('')
                    logging.debug ('jsondata loaded')
                
                status = jsondata['status']
                msg = jsondata['msg']
                
                if debug:
                    logging.debug ('')
                    logging.debug (f'status: {status:s}')
                    logging.debug (f'msg: {msg:s}')

            except Exception:
                msg = 'returned JSON object parse error'
                
                if debug:
                    logging.debug ('')
                    logging.debug ('JSON object parse error')
      
                
            raise Exception (msg)
            
            if debug:
                logging.debug ('')
                logging.debug (f'msg= {msg:s}')
     
        return (response.text)
#
#}  end Archive.__make_query
#

#
#}  end class Archive
#
 

class objLookup:
#
#{ objLookup class
#
    """
    objLookup wraps ExoPlanet's web name resolver into a python class; 
    the exoLookup checks the exoplanet archive database and if that fails 
    it checks with the Sesame web service at CDS.  Sesame checks the CDS
    database and if that fails it checks NED.  So this class covers
    SIMBAD, NED, and ExoPlanet search.

    Required input:

        object (char):  object name to be resolved
    """


    lookupurl = 'https://exoplanetarchive.ipac.caltech.edu/cgi-bin/Lookup/nph-lookup?'
    msg = ''
    status = ''

    url = ''
    response = None 

    source = ''
    input = ''
    objname = ''
    objtype = ''
    parsename= ''
    objdesc = ''
    ra2000= ''
    dec2000 = ''
    cra2000 = ''
    cdec2000 = ''

    debug = 0

    def __init__ (self, object, **kwargs):
#
#{ objLookup.init
#

        self.object = object

        if ('debug' in kwargs):
            self.debug = kwargs['debug']

        self.url = self.lookupurl + 'location=' + self.object

        if self.debug:
            logging.debug ('')
            logging.debug (f'url={self.url:s}')


        self.response = None 
        try:
            self.response = requests.get (self.url, stream=True)

            if self.debug:
                logging.debug ('')
                logging.debug (f'response:')
                logging.debug (self.response)

        except Exception as e:
            self.msg = f'submit request exception: {str(e):s}'
            raise Exception (self.msg)

        if self.debug:
            logging.debug ('')
            logging.debug (
                f'response.statu_code= {self.response.status_code:d}')

            logging.debug ('response.headers:')
            logging.debug (self.response.headers)

            logging.debug ('response.text:')
            logging.debug (self.response.text)


        content_type = ''
        try:
            content_type = self.response.headers['Content-type']
        
            if self.debug:
                logging.debug ('')
                logging.debug (f'content_type= {content_type:s}')

        except Exception as e:
            self.msg = f'extract content_type exception: {str(e):s}'
            raise Exception (self.msg)


        jsondata = None
        try:
            jsondata = json.loads (self.response.text)

        except Exception as e:
            self.msg = f'load jsondata exception: {str(e):s}'
            raise Exception (self.msg)

        if self.debug:
            logging.debug ('')
            logging.debug ('jsondata:')
            logging.debug (jsondata)

        
        self.status = ''
        try:
            self.status = jsondata['stat']
            if self.debug:
                logging.debug ('')
                logging.debug (f'self.status= {self.status:s}')

        except Exception as e:

            self.msg = f'extract stat exception: {str(e):s}'
            if self.debug:
                logging.debug ('')
                logging.debug (f'self.msg= {self.msg:s}')
            
            raise Exception (self.msg)

        if self.debug:
            logging.debug ('')
            logging.debug (f'got here: status= {self.status:s}')
       
    
        if (self.status.lower() == 'ok'):
#
#{  objLookup OK, extract parameters
        
            try:
                self.source = jsondata['source']
            except Exception as e:
                if self.debug:
                    logging.debug ('')
                    logging.debug (f'extract source exception: {str(e):s}')
    
            try:
                self.objname = jsondata['objname']
            except Exception as e:
                if self.debug:
                    logging.debug ('')
                    logging.debug (f'extract objname exception: {str(e):s}')
                
            try:
                self.objtype = jsondata['objtype']
            except Exception as e:
                if self.debug:
                    logging.debug ('')
                    logging.debug (f'extract objtype exception: {str(e):s}')
                
            try:
                self.objdesc = jsondata['objdesc']
            except Exception as e:
                if self.debug:
                    logging.debug ('')
                    logging.debug (f'extract objdesc exception: {str(e):s}')
                
            try:
                self.parsename = jsondata['parsename']
            except Exception as e:
                if self.debug:
                    logging.debug ('')
                    logging.debug (f'extract parsename exception: {str(e):s}')
                
            try:
                self.ra2000 = jsondata['ra2000']
            except Exception as e:
                if self.debug:
                    logging.debug ('')
                    logging.debug (f'extract ra2000 exception: {str(e):s}')
                
            try:
                self.dec2000 = jsondata['dec2000']
            except Exception as e:
                if self.debug:
                    logging.debug ('')
                    logging.debug (f'extract dec2000 exception: {str(e):s}')
                
            try:
                self.cra2000 = jsondata['cra2000']
            except Exception as e:
                if self.debug:
                    logging.debug ('')
                    logging.debug (f'extract cra2000 exception: {str(e):s}')
                
            try:
                self.cdec2000 = jsondata['cdec2000']
            except Exception as e:
                if self.debug:
                    logging.debug ('')
                    logging.debug (f'extract cdec20000 exception: {str(e):s}')
                
            if self.debug:
                logging.debug ('')
                
                logging.debug (f'dec2000= {self.dec2000:s}')
                logging.debug (f'source= {self.source:s}')
                logging.debug (f'objname= {self.objname:s}')
                logging.debug (f'objtype= {self.objtype:s}')
                logging.debug (f'objdesc= {self.objdesc:s}')
                logging.debug (f'parsename= {self.parsename:s}')
                logging.debug (f'ra2000= {self.ra2000:s}')
                logging.debug (f'dec2000= {self.dec2000:s}')
                logging.debug (f'cra2000= {self.cra2000:s}')
                logging.debug (f'cdec2000= {self.cdec2000:s}')

#
#}  end objLookup OK, extract parameters
#
        else:
#
#{  objLookup Error, extract errmsg
#
            self.status = 'error'
            try:
                self.msg = jsondata['msg']
                
                if self.debug:
                    logging.debug ('')
                    logging.debug (f'errmsg= {self.msg:s}')
        
            except Exception as e:

                self.msg = f'extract msg exception: {str(e):s}'
                raise Exception (self.msg)

        if self.debug:
            logging.debug ('')
            logging.debug ('got here3')
        
#
#}  end extract errmsg
#
        return
#
#} end objLookup.init
#
#
#} end objLookup class
#



class KoaTap:
#
#{ KoaTap class
#

    """
    KoaTap class provides client access to KOA's TAP service.   

    Public data does not require user login, optional KOA login via 
    KoaLogin class are used to search for a user's proprietary data.

    Calling Synopsis (example):

    service = KoaTap (url, cookiefile=cookiepath)

    job = service.send_async (query, format='votable', request='doQuery', ...)

    or
    
    job = service.send_sync (query, format='votable', request='doQuery', ...)

    required parameter
    ------------------ 
    query -- a SQL statement in specified query language;

    optional paramters
    ------------------    
    cookiefile -- a full path cookie file containing user info; 
                  default is no cookiefile
    
    request -- default 'doQuery',
    lang    -- default 'ADQL',
    phase   -- default 'RUN',
    format  -- default 'ipac',
    maxrec  -- maximum records to be returned 
	       default: -1 or not specified will return all requested records
       
    debug      -- default is no debug written
    """

    def __init__ (self, url, **kwargs):
#
#{ KoaTap.init
#

        self.url = url 
        self.cookiename = ''
        self.cookiepath = ''
        self.async_job = 0 
        self.sync_job = 0
        
        self.response = None 
        self.response_result = None 
              
        
        self.outpath = ''
        
        self.debug = 0  
 
        self.datadict = dict()
        
        self.status = ''
        self.msg = ''

#
#    koajob contains async job's status;
#    resulttbl is the result of sync saved an astropy table 
#
        self.koajob = None
        self.astropytbl = None
        
        if ('debug' in kwargs):
            self.debug = kwargs.get('debug') 
 
        if self.debug:
            logging.debug ('')
            logging.debug ('')
            logging.debug ('Enter koatap.init (debug on)')
                                
        if ('cookiefile' in kwargs):
            self.cookiepath = kwargs.get('cookiefile')

        if self.debug:
            logging.debug ('')
            logging.debug (f'cookiepath= {self.cookiepath:s}')

        self.request = 'doQuery'
        if ('request' in kwargs):
            self.request = kwargs.get('request')

        self.lang = 'ADQL'
        if ('lang' in kwargs):
            self.lang = kwargs.get('lang')

        self.phase = 'RUN'
        if ('phase' in kwargs):
           self.phase = kwargs.get('phase')

        self.format = 'votable'
        if ('format' in kwargs):
           self.format = kwargs.get('format')

        self.maxrec = -1 
        if ('maxrec' in kwargs):
           self.maxrec = kwargs.get('maxrec')

        if self.debug:
            logging.debug ('')
            logging.debug (f'url= {self.url:s}')
            logging.debug (f'cookiepath= {self.cookiepath:s}')
            logging.debug (f'self.maxrec= {self.maxrec:d}')

#
#    turn on server debug
#   
        pid = os.getpid()
        self.datadict['request'] = self.request              
        self.datadict['lang'] = self.lang              
        self.datadict['phase'] = self.phase              
        self.datadict['format'] = self.format              
        self.datadict['maxrec'] = self.maxrec              

        for key in self.datadict:

            if self.debug:
                logging.debug ('')
                logging.debug (f'key= {key:s} val= {str(self.datadict[key]):s}')
    
        self.datadict['debug'] = 1              
        
        self.cookiejar = http.cookiejar.MozillaCookieJar (self.cookiepath)
         
        if self.debug:
            logging.debug ('')
            logging.debug ('cookiejar')
            logging.debug (self.cookiejar)
   
        if (len(self.cookiepath) > 0):
        
            try:
                self.cookiejar.load (ignore_discard=True, ignore_expires=True);
            
                if self.debug:
                    logging.debug (
                        'cookie loaded from %s' % self.cookiepath)
        
                    for cookie in self.cookiejar:
                        logging.debug ('cookie:')
                        logging.debug (cookie)
                        
                        logging.debug (f'cookie.name= {cookie.name:s}')
                        logging.debug (f'cookie.value= {cookie.value:s}')
                        logging.debug (f'cookie.domain= {cookie.domain:s}')
            except:
                if self.debug:
                    logging.debug ('KoaTap: loadCookie exception')
 
                self.msg = 'Error: failed to load cookie file.'
                raise Exception (self.msg) 

        return 
#
#} end KoaTap.init
#
       

    def send_async (self, query, **kwargs):
#
#{ KoaTap.send_async
#
       
        debug = 0

        if ('debug' in kwargs):
            debug = kwargs.get('debug') 
 
        if debug:
            logging.debug ('')
            logging.debug ('Enter send_async:')
 
        self.async_job = 1
        self.sync_job = 0

        url = self.url + '/async'

        if debug:
            logging.debug ('')
            logging.debug (f'url= {url:s}')
            logging.debug (f'query= {query:s}')

        self.datadict['query'] = query 

        self.maxrec = -1 

        if ('format' in kwargs):
            
            self.format = kwargs.get('format')
            self.datadict['format'] = self.format              

            if debug:
                logging.debug ('')
                logging.debug (f'format= {self.format:s}')
            
        if ('maxrec' in kwargs):
            
            self.maxrec = kwargs.get('maxrec')
            self.datadict['maxrec'] = self.maxrec              
            
            if debug:
                logging.debug ('')
                logging.debug (f'maxrec= {self.maxrec:d}')
        
        if ('propflag' in kwargs):
            
            self.propflag = kwargs.get('propflag')
            self.datadict['propflag'] = self.propflag              
            
            if debug:
                logging.debug ('')
                logging.debug (f'propflag= {self.propflag:d}')
        
        self.oupath = ''
        if ('outpath' in kwargs):
            self.outpath = kwargs.get('outpath')
  
        try:

            if (len(self.cookiepath) > 0):
        
                self.response = requests.post (url, data= self.datadict, \
	            cookies=self.cookiejar, allow_redirects=False)
            else: 
                self.response = requests.post (url, data= self.datadict, \
	            allow_redirects=False)

            if debug:
                logging.debug ('')
                logging.debug ('request sent')

        except Exception as e:
           
            self.status = 'error'
            self.msg = str(e)
	    
            if debug:
                logging.debug ('')
                logging.debug (f'exception: e= {str(e):s}')
            
            return (self.msg)


        if debug:
            logging.debug ('')
            logging.debug (f'status_code= {self.response.status_code:d}')
            logging.debug ('self.response: ')
            logging.debug (self.response)
            logging.debug ('self.response.headers: ')
            logging.debug (self.response.headers)
            logging.debug ('')
            logging.debug (f'status_code= {self.response.status_code:d}')
            
#
# {   if status_code != 303: probably error message
#
        if (self.response.status_code != 303):
            
            if debug:
                logging.debug ('')
                logging.debug ('case: not re-direct')
       
            self.content_type = self.response.headers['Content-type']
            self.encoding = self.response.encoding
        
            if debug:
                logging.debug ('')
                logging.debug (f'content_type= {self.content_type:s}')
                logging.debug ('encoding= ')
                logging.debug (self.encoding)


            data = None
            self.status = ''
            self.msg = ''
           
            if debug:
                logging.debug ('')
                logging.debug ('self.response:')
                logging.debug (self.response.text)
      
            if (self.content_type == 'application/json'):
                
                if debug:
                    logging.debug ('')
                    logging.debug ('case json errmsg:')
      
                try:
                    data = self.response.json()
                    
                except Exception as e:
                
                    if debug:
                        logging.debug ('')
                        logging.debug (f'JSON object parse error: {str(e):s}')
      
                    self.status = 'error'
                    self.msg = 'JSON parse error: ' + str(e)
                
                    if debug:
                        logging.debug ('')
                        logging.debug (f'status= {self.status:s}')
                        logging.debug (f'msg= {self.msg:s}')

                    return (self.response.text)

                self.status = data['status']
                self.msg = data['msg']
                
                if debug:
                    logging.debug ('')
                    logging.debug (f'status= {self.status:s}')
                    logging.debug (f'msg= {self.msg:s}')

                return (self.msg)

            elif (self.content_type == 'text/xml'):

                if debug:
                    logging.debug ('')
                    logging.debug ('case xml errmsg:')
      
                self.msg = ''
                try:
                    self.msg = self.extract_xmlerr (self.response.text)
                    
                    if debug:
                        logging.debug ('')
                        logging.debug (f'returned extract_xmlerr: {self.msg:s}')
            
                    return (self.msg)

                except Exception as e:

                    if debug:
                        logging.debug ('')
                        logging.debug (f'parse errmsg exception: {str(e):s}')
    
                    return (self.response.text)

            else:
                return (self.response.text)
        
        if debug:
            logging.debug ('')
            logging.debug ('here')
    
#
#} end dealing with status_code != 303
#

#
#    retrieve statusurl
#
        self.statusurl = ''
        if (self.response.status_code == 303):
            self.statusurl = self.response.headers['Location']

        if debug:
            logging.debug ('')
            logging.debug (f'statusurl= {self.statusurl:s}')

        if (len(self.statusurl) == 0):
            self.msg = 'Error: failed to retrieve statusurl from re-direct'
            return (self.msg)

#
#    create koajob to save status result
#
        try:
            if debug:
                self.koajob = KoaJob (\
                    self.statusurl, debug=1)
            else:
                self.koajob = KoaJob (\
                    self.statusurl)
        
            if debug:
                logging.debug ('')
                logging.debug (f'koajob instantiated')
                logging.debug (f'phase= {self.koajob.phase:s}')
       
       
        except Exception as e:
           
            self.status = 'error'
            self.msg = str(e)
	    
            if debug:
                logging.debug ('')
                logging.debug (f'exception: e= {str(e):s}')
            
            return (self.msg)    
        
#
#    loop until job is complete and download the data
#
        
        phase = self.koajob.phase
        
        if debug:
            logging.debug ('')
            logging.debug (f'phase: {phase:s}')
            
        if ((phase.lower() != 'completed') and (phase.lower() != 'error')):
            
            while ((phase.lower() != 'completed') and \
                (phase.lower() != 'error')):
                
                time.sleep (2)
                phase = self.koajob.get_phase()
        
                if debug:
                    logging.debug ('')
                    logging.debug ('here0-1')
                    logging.debug (f'phase= {phase:s}')
            
        if debug:
            logging.debug ('')
            logging.debug ('here0-2')
            logging.debug (f'phase= {phase:s}')
            
#
#    phase == 'error'
#
        if (phase.lower() == 'error'):
	   
            self.status = 'error'
            self.msg = self.koajob.errorsummary
        
            if debug:
                logging.debug ('')
                logging.debug (f'returned get_errorsummary: {self.msg:s}')
            
            return (self.msg)

        if debug:
            logging.debug ('')
            logging.debug ('here2: phase is completed')
            
#
#   phase == 'completed' 
#
        self.resulturl = self.koajob.resulturl
        if debug:
            logging.debug ('')
            logging.debug (f'resulturl= {self.resulturl:s}')

#
#   send resulturl to retrieve result table
#
        try:
            self.response_result = requests.get (self.resulturl, stream=True)
        
            if debug:
                logging.debug ('')
                logging.debug ('resulturl request sent')

        except Exception as e:
           
            self.status = 'error'
            self.msg = str(e)
	    
            if debug:
                logging.debug ('')
                logging.debug (f'exception: e= {str(e):s}')
            
            raise Exception (self.msg)    
     
       
#
# save table to file
#
        if debug:
            logging.debug ('')
            logging.debug ('got here')

        self.msg = self.save_data (self.outpath)
            
        if debug:
            logging.debug ('')
            logging.debug (f'returned save_data: msg= {self.msg:s}')

        return (self.msg)


#
#    outpath is not given: return resulturl
#
        """
        if (len(self.outpath) == 0):
           
            self.resulturl = self.koajob.resulturl
            if debug:
                logging.debug ('')
                logging.debug (f'resulturl= {self.resulturl:s}')

            return (self.resulturl)

        try:
            self.koajob.get_result (self.outpath)

            if debug:
                logging.debug ('')
                logging.debug (f'returned self.koajob.get_result')
        
        except Exception as e:
            
            self.status = 'error'
            self.msg = str(e)
	    
            if debug:
                logging.debug ('')
                logging.debug (f'exception: e= {str(e):s}')
            
            return (self.msg)    
        
        if debug:
            logging.debug ('')
            logging.debug ('got here: download result successful')
      
        self.status = 'ok'
        self.msg = 'Result downloaded to file: [' + self.outpath + ']'
	    
        if debug:
            logging.debug ('')
            logging.debug (f'self.msg = {self.msg:s}')
       
        
	self.msg = self.save_data (self.outpath)
            
	
        if debug:
            logging.debug ('')
            logging.debug (f'returned save_data: msg= {self.msg:s}')


        return (self.msg) 
        """
#
#} end KoaTap.send_async
#


    def send_sync (self, query, **kwargs):
#
#{ KoaTap.send_sync
#
      
        debug = 0

        if debug:
            logging.debug ('')
            logging.debug ('Enter send_sync:')
            logging.debug (f'query= {query:s}')
 
        url = self.url + '/sync'

        if debug:
            logging.debug ('')
            logging.debug (f'url= {url:s}')

        self.sync_job = 1
        self.async_job = 0
        self.datadict['query'] = query
    
#
#    optional parameters: format, maxrec, self.outpath
#
        self.maxrec = -1 

        if ('format' in kwargs):
            
            self.format = kwargs.get('format')
            self.datadict['format'] = self.format              

        
            if debug:
                logging.debug ('')
                logging.debug (f'format= {self.format:s}')
            
        if ('maxrec' in kwargs):
            
            self.maxrec = kwargs.get('maxrec')
            self.datadict['maxrec'] = self.maxrec              
            
            if debug:
                logging.debug ('')
                logging.debug (f'maxrec= {self.maxrec:d}')
        
        self.outpath = ''
        if ('outpath' in kwargs):
            self.outpath = kwargs.get('outpath')
        
        if debug:
            logging.debug ('')
            logging.debug (f'outpath= {self.outpath:s}')
	
        try:
            if (len(self.cookiepath) > 0):
        
                self.response = requests.post (url, data= self.datadict, \
                    cookies=self.cookiejar, allow_redirects=False, stream=True)
            else: 
                self.response = requesrs.post (url, data= self.datadict, \
                    allow_redicts=False, stream=True)

            if debug:
                logging.debug ('')
                logging.debug ('request sent')

        except Exception as e:
           
            self.status = 'error'
            self.msg = str(e)

            if debug:
                logging.debug ('')
                logging.debug (f'exception: e= {str(e):s}')
            
            return (self.msg)

#
#    re-direct case not implemented for send_sync
#
#	if (self.response.status_code == 303):
#            self.resulturl = self.response.headers['Location']
        
        self.content_type = self.response.headers['Content-type']
        self.encoding = self.response.encoding

        if debug:
            logging.debug ('')
            logging.debug (f'content_type= {self.content_type:s}')
       
        data = None
        self.status = ''
        self.msg = ''
        if (self.content_type == 'application/json'):
#
#    error message
#
            try:
                data = self.response.json()
            except Exception:
                if debug:
                    logging.debug ('')
                    logging.debug ('JSON object parse error')
      
                self.status = 'error'
                self.msg = 'returned JSON object parse error'
                
                return (self.msg)
            
            if debug:
                logging.debug ('')
                logging.debug (f'status= {self.status:s}')
                logging.debug (f'msg= {self.msg:s}')
     
#
# download resulturl and save table to file
#
        if debug:
            logging.debug ('')
            logging.debug ('send request to get resulturl')

#
# save table to file
#
        if debug:
            logging.debug ('')
            logging.debug ('got here')

        self.msg = self.save_data (self.outpath)
            
        if debug:
            logging.debug ('')
            logging.debug (f'returned save_data: msg= {self.msg:s}')

        return (self.msg)
#
#} end KoaTap.send_sync
#


#
# extract errmsg from xml return
#
    def extract_xmlerr (self, xmlstruct):
#
#{ KoaTap.extract_xmlerr
#
        debug = 0

        if debug:
            logging.debug ('')
            logging.debug ('Enter extract_xmlerr:')
            logging.debug (f'xmlstruct= {xmlstruct:s}')
      
#
#    convert status xml structure to dictionary doc 
#
        doc = None
        try:
            doc = xmltodict.parse (xmlstruct)

        except Exception as e:

            self.msg = 'Failed to parse xmltodict: ' + str(e)

            if self.debug:
                logging.debug ('')
                logging.debug (f'exception: e= {str(e):s}')

            raise Exception (self.msg)

        if self.debug:
            logging.debug ('')
            logging.debug ('doc: ')
            logging.debug (doc)
        
#
#    check if this is a error message: in the structure of a votable
#
        votbl = None
        try: 
            votbl = doc['VOTABLE']
	
        except Exception as e:
           
            self.msg = 'Failed to extract votbl from doc '
	    
            if self.debug:
                logging.debug ('')
                logging.debug (f'exception: e= {str(e):s}')
            raise Exception (self.msg)    
        
        if self.debug:
            logging.debug ('')
            logging.debug ('votbl found so it is an errmsg')
            logging.debug (votbl)

        
        if (votbl is None):
            self.msg = 'Not a votbl format.'
	    
            if self.debug:
                logging.debug ('')
                logging.debug (f'exception: e= {str(e):s}')
            
            raise Exception (self.msg)    
     
        
        info = None
        try: 
            info = votbl['RESOURCE']['INFO']

        except Exception as e:
           
            self.msg = 'Failed to extract INFO from doc '
	    
            if self.debug:
                logging.debug ('')
                logging.debug (f'exception: e= {str(e):s}')
            
            raise Exception (self.msg)    
     
        if self.debug:
            logging.debug ('')
            logging.debug ('info found: extract errmsg')
            logging.debug (info)
        
        if (info is None):
            
            self.msg = 'No error message found.'
            
            if self.debug:
                logging.debug ('')
                logging.debug (f'self.msg= {self.msg:s}')
            
            raise Exception (self.msg)    
     
        
        infoval = ''
        errmsg = ''
        try: 
            infoval = info['@value'] 
            errmsg = info['#text'] 
	
        except Exception as e:
           
            self.msg = 'Failed to extract infoval and text from doc '
	    
            if self.debug:
                logging.debug ('')
                logging.debug (f'exception: e= {str(e):s}')
            
            raise Exception (self.msg)    
     
        if self.debug:
            logging.debug ('')
            logging.debug (f'infoval= {infoval:s}')
            logging.debug (f'errmsg= {errmsg:s}')

        if (infoval.lower() != 'error'):
            
            self.msg = 'No error message found.'
        
            if self.debug:
                logging.debug ('')
                logging.debug (f'infoval not error: {infoval.lower():s}')

            raise Exception (self.msg)    
        
        return (errmsg)    

#
#} end KoaTap.extract_xmlerr
#


#
# save data to astropy table
#
    def save_data (self, outpath):
#
#{ KoaTap.save_date
#

        debug = 0

        if debug:
            logging.debug ('')
            logging.debug ('Enter save_data:')
            logging.debug (f'outpath= {outpath:s}')
      
        tmpfile_created = 0

        fpath = ''
        if (len(outpath) >  0):
            fpath = outpath
        else:
            fd, fpath = tempfile.mkstemp(suffix='.xml', dir='./')
            tmpfile_created = 1 
            
            if debug:
                logging.debug ('')
                logging.debug (f'tmpfile_created = {tmpfile_created:d}')

        if debug:
            logging.debug ('')
            logging.debug (f'fpath= {fpath:s}')
    
        try:
            fp = open (fpath, "wb")
        
        except Exception as e:

            if debug:
                logging.debug ('')
                logging.debug (f'save_data error: {str(e):s}')
            
            self.msg = 'Failed to open file [' + fpath + '] for write.'
            return (self.msg)

        
        try:
            for data in self.response_result.iter_content(4096):
                
                len_data = len(data)            
        
                if (len_data < 1):
                    break

                fp.write (data)
        
            fp.close()

        except Exception as e:

            if debug:
                logging.debug ('')
                logging.debug (f'save_data error: {str(e):s}')
            
            self.msg = 'save_data error: ' + str(e)
            return (self.msg)

        if debug:
            logging.debug ('')
            logging.debug (f'data written to file: {fpath:s}')
                
        if (len(self.outpath) >  0):
            self.msg = 'Result downloaded to file [' + self.outpath + ']'
        else:
#
#    read temp outpath to astropy table
#
            self.astropytbl = Table.read (fpath, format='votable')	    
            self.msg = 'Result saved in memory (astropy table).'
      
        if debug:
            logging.debug ('')
            logging.debug (f'{self.msg:s}')
     
        if (tmpfile_created == 1):
            os.remove (fpath)
            
            if debug:
                logging.debug ('')
                logging.debug ('tmpfile {fpath:s} deleted')

        return (self.msg)
#
#} end KoaTap.save_date
#


    def print_data (self):
#
#{ KoaTap.print_date
#

        debug = 0

        if debug:
            logging.debug ('')
            logging.debug ('Enter print_data:')

        try:

            """
            len_table = len (self.astropytbl)
        
            if debug:
                logging.debug ('')
                logging.debug (f'len_table= {len_table:d}')
       
            for i in range (len_table):
	    
                row = self.astropytbl[i]
                print (row)
            """

            self.astropytbl.pprint()

        except Exception as e:
            
            raise Exception (str(e))

        return
#
#} end KoaTap.print_data
#



#
#    outpath is given: loop until job is complete and download the data
#
    def get_data (self, resultpath):
#
#{ KoaTap.get_data
#

        debug = 0
        
        if debug:
            logging.debug ('')
            logging.debug ('Enter get_data:')
            logging.debug (f'async_job = {self.async_job:d}')
            logging.debug (f'resultpath = {resultpath:s}')



        if (self.async_job == 0):
#
#    sync data is in astropytbl
#
            self.astropytbl.write (resultpath)

            if debug:
                logging.debug ('')
                logging.debug ('astropytbl written to resultpath')

            self.msg = 'Result written to file: [' + resultpath + ']'
        
        else:
            phase = self.koajob.get_phase()
        
            if debug:
                logging.debug ('')
                logging.debug (f'returned koajob.get_phase: phase= {phase:s}')

            while ((phase.lower() != 'completed') and \
	        (phase.lower() != 'error')):
                time.sleep (2)
                phase = self.koajob.get_phase()
        
                if debug:
                    logging.debug ('')
                    logging.debug (\
                        f'returned koajob.get_phase: phase= {phase:s}')

#
#    phase == 'error'
#
            if (phase.lower() == 'error'):
	   
                self.status = 'error'
                self.msg = self.koajob.errorsummary
        
                if debug:
                    logging.debug ('')
                    logging.debug (f'returned get_errorsummary: {self.msg:s}')
            
                return (self.msg)

#
#   job completed write table to disk file
#
            try:
                self.koajob.get_result (resultpath)

                if debug:
                    logging.debug ('')
                    logging.debug (f'returned koajob.get_result')
        
            except Exception as e:
            
                self.status = 'error'
                self.msg = str(e)
	    
                if debug:
                    logging.debug ('')
                    logging.debug (f'exception: e= {str(e):s}')
            
                return (self.msg)    
        
            if debug:
                logging.debug ('')
                logging.debug ('got here: download result successful')

            self.status = 'ok'
            self.msg = 'Result downloaded to file: [' + resultpath + ']'

        if debug:
            logging.debug ('')
            logging.debug (f'self.msg = {self.msg:s}')
       
        return (self.msg) 
#
#} end KoaTap.get_data
#

#
#} end class KoaTap
#

class KoaJob:
#
#{ class KoaJob
#

    """
    KoaJob class is used internally by KoaTap class to store the job 
    parameters and returned urls for job status and result files.  
    """

    def __init__ (self, statusurl, **kwargs):
#
#{ KoaJob.init
#

        self.debug = 0 
        
        self.statusurl = statusurl

        self.status = ''
        self.msg = ''
        
        self.statusstruct = ''
        self.job = ''


        self.jobid = ''
        self.processid = ''
        self.ownerid = 'None'
        self.quote = 'None'
        self.phase = ''
        self.starttime = ''
        self.endtime = ''
        self.executionduration = ''
        self.destruction = ''
        self.errorsummary = ''
        
        self.parameters = ''
        self.resulturl = ''

        if ('debug' in kwargs):
           
            self.debug = kwargs.get('debug')
           
        if self.debug:
            logging.debug ('')
            logging.debug ('Enter koajob (debug on)')
                                
        try:
            self.__get_statusjob()
         
            if self.debug:
                logging.debug ('')
                logging.debug ('returned __get_statusjob')

        except Exception as e:
           
            self.status = 'error'
            self.msg = str(e)
	    
            if self.debug:
                logging.debug ('')
                logging.debug (f'exception: e= {str(e):s}')
            
            raise Exception (self.msg)    
        
        if self.debug:
            logging.debug ('')
            logging.debug ('done KoaJob.init:')

        return     
#
#} end KoaJob.init
#

   
    def get_status (self):
#
#{ KoaJob.get_status
#

        if self.debug:
            logging.debug ('')
            logging.debug ('Enter get_status')
            logging.debug (f'phase= {self.phase:s}')

        if (self.phase.lower() != 'completed'):

            try:
                self.__get_statusjob ()

                if self.debug:
                    logging.debug ('')
                    logging.debug ('returned get_statusjob:')
                    logging.debug ('job= ')
                    logging.debug (self.job)

            except Exception as e:
           
                self.status = 'error'
                self.msg = str(e)
	    
                if self.debug:
                    logging.debug ('')
                    logging.debug (f'exception: e= {str(e):s}')
                 
                raise Exception (self.msg)   

        return (self.statusstruct)
#
#} end KoaJob.get_status
#


    def get_resulturl (self):
#
#{ KoaJob.get_resulturl
#

        if self.debug:
            logging.debug ('')
            logging.debug ('Enter get_resulturl')
            logging.debug (f'phase= {self.phase:s}')

        if (self.phase.lower() != 'completed'):

            try:
                self.__get_statusjob ()

                if self.debug:
                    logging.debug ('')
                    logging.debug ('returned get_statusjob:')
                    logging.debug ('job= ')
                    logging.debug (self.job)

            except Exception as e:
           
                self.status = 'error'
                self.msg = str(e)
	    
                if self.debug:
                    logging.debug ('')
                    logging.debug (f'exception: e= {str(e):s}')
                 
                raise Exception (self.msg)   

        return (self.resulturl)
#
#} end KoaJob.get_resulturl
#


    def get_result (self, outpath):
#
#{ KoaJob.get_result
#

        if self.debug:
            logging.debug ('')
            logging.debug ('Enter get_result')
            logging.debug (f'resulturl= {self.resulturl:s}')
            logging.debug (f'outpath= {outpath:s}')

        if (len(outpath) == 0):
            self.status = 'error'
            self.msg = 'Output file path is required.'
            return

        
        if (self.phase.lower() != 'completed'):

            try:
                self.__get_statusjob ()

                if self.debug:
                    logging.debug ('')
                    logging.debug ('returned __get_statusjob')
                    logging.debug (f'resulturl= {self.resulturl:s}')

            except Exception as e:
           
                self.status = 'error'
                self.msg = str(e)
	    
                if self.debug:
                    logging.debug ('')
                    logging.debug (f'exception: e= {str(e):s}')
                
                raise Exception (self.msg)    
    

        if (len(self.resulturl) == 0):
  
            self.get_resulturl()            
            self.msg = 'Failed to retrieve resulturl from status structure.'
            raise Exception (self.msg)    
	    

#
#   send resulturl to retrieve result table
#
        try:
            response = requests.get (self.resulturl, stream=True)
        
            if self.debug:
                logging.debug ('')
                logging.debug ('resulturl request sent')

        except Exception as e:
           
            self.status = 'error'
            self.msg = str(e)
	    
            if self.debug:
                logging.debug ('')
                logging.debug (f'exception: e= {str(e):s}')
            
            raise Exception (self.msg)    
     
#
# retrieve table from response
#
        with open (outpath, "wb") as fp:
            
            for data in response.iter_content(4096):
                
                len_data = len(data)            
            
#                if debug:
#                    logging.debug ('')
#                    logging.debug (f'len_data= {len_data:d}')
 
                if (len_data < 1):
                    break

                fp.write (data)
        fp.close()
        
        self.resultpath = outpath
        self.status = 'ok'
        self.msg = 'returned table written to output file: ' + outpath
        
        if self.debug:
            logging.debug ('')
            logging.debug ('done writing result to file')
            
        return        
#
#} end KoaJob.get_result
#

    
    def get_parameters (self):
#
#{ KoaJob.get_parameters
#

        if self.debug:
            logging.debug ('')
            logging.debug ('Enter get_parameters')
            logging.debug ('parameters:')
            logging.debug (self.parameters)

        return (self.parameters)
#
#} end KoaJob.get_parameters
#
    

    def get_phase (self):
#
#{ KoaJob.get_phase
#


        if self.debug:
            logging.debug ('')
            logging.debug ('Enter get_phase')
            logging.debug (f'self.phase= {self.phase:s}')

        if ((self.phase.lower() != 'completed') and \
	    (self.phase.lower() != 'error')):

            try:
                self.__get_statusjob ()

                if self.debug:
                    logging.debug ('')
                    logging.debug ('returned get_statusjob:')
                    logging.debug ('job= ')
                    logging.debug (self.job)

            except Exception as e:
           
                self.status = 'error'
                self.msg = str(e)
	    
                if self.debug:
                    logging.debug ('')
                    logging.debug (f'exception: e= {str(e):s}')
                 
                raise Exception (self.msg)   

            if self.debug:
                logging.debug ('')
                logging.debug (f'phase= {self.phase:s}')

        return (self.phase)
#
#} end KoaJob.get_phase
#
    
    
    def get_jobid (self):
#
#{ KoaJob.get_jobid
#

        if self.debug:
            logging.debug ('')
            logging.debug ('Enter get_jobid')

        if (len(self.jobid) == 0):
            self.jobid = self.job['uws:jobId']

        if self.debug:
            logging.debug ('')
            logging.debug (f'jobid= {self.jobid:s}')

        return (self.jobid)
#
#} end KoaJob.get_jobid
#
    
    def get_processid (self):
#
#{ KoaJob.get_processid
#

        if self.debug:
            logging.debug ('')
            logging.debug ('Enter get_processid')

        if (len(self.processid) == 0):
            self.processid = self.job['uws:processId']

        if self.debug:
            logging.debug ('')
            logging.debug (f'processid= {self.processid:s}')

        return (self.processid)
#
#} end KoaJob.get_processid
#

    
    """ 
    def get_ownerid (self):
        return ('None')

    def get_quote (self):
        return ('None')
    """

    def get_starttime (self):
#
#{ KoaJob.get_starttime
#

        if self.debug:
            logging.debug ('')
            logging.debug ('Enter get_starttime')

        if (len(self.starttime) == 0):
            self.starttime = self.job['uws:startTime']

        if self.debug:
            logging.debug ('')
            logging.debug (f'starttime= {self.starttime:s}')

        return (self.starttime)
#
#} end KoaJob.get_starttime
#


    def get_endtime (self):
#
#{ KoaJob.get_endtime
#

        if self.debug:
            logging.debug ('')
            logging.debug ('Enter get_endtime')

        if (self.phase.lower() != 'completed'):

            try:
                self.__get_statusjob ()

                if self.debug:
                    logging.debug ('')
                    logging.debug ('returned get_statusjob:')
                    logging.debug ('job= ')
                    logging.debug (self.job)

            except Exception as e:
           
                self.status = 'error'
                self.msg = str(e)
	    
                if self.debug:
                    logging.debug ('')
                    logging.debug (f'exception: e= {str(e):s}')
                 
                raise Exception (self.msg)   

        self.endtime = self.job['uws:endTime']

        if self.debug:
            logging.debug ('')
            logging.debug (f'endtime= {self.endtime:s}')

        return (self.endtime)
#
#} end KoaJob.get_endtime
#


    def get_executionduration (self):
#
#{ KoaJob.get_executionduration
#

        if self.debug:
            logging.debug ('')
            logging.debug ('Enter get_executionduration')

        
        if (self.phase.lower() != 'completed'):

            try:
                self.__get_statusjob ()

                if self.debug:
                    logging.debug ('')
                    logging.debug ('returned get_statusjob:')
                    logging.debug ('job= ')
                    logging.debug (self.job)

            except Exception as e:
           
                self.status = 'error'
                self.msg = str(e)
	    
                if self.debug:
                    logging.debug ('')
                    logging.debug (f'exception: e= {str(e):s}')
                 
                raise Exception (self.msg)   

        self.executionduration = self.job['uws:executionDuration']

        if self.debug:
            logging.debug ('')
            logging.debug (f'executionduration= {self.executionduration:s}')

        return (self.executionduration)
#
#} KoaJob.get_executionduration
#


    def get_destruction (self):
#
#{ KoaJob.get_destruction
#


        if self.debug:
            logging.debug ('')
            logging.debug ('Enter get_destruction')

        if (self.phase.lower() != 'completed'):

            try:
                self.__get_statusjob ()

                if self.debug:
                    logging.debug ('')
                    logging.debug ('returned get_statusjob:')
                    logging.debug ('job= ')
                    logging.debug (self.job)

            except Exception as e:
           
                self.status = 'error'
                self.msg = str(e)
	    
                if self.debug:
                    logging.debug ('')
                    logging.debug (f'exception: e= {str(e):s}')
                 
                raise Exception (self.msg)   

        self.destruction = self.job['uws:destruction']

        if self.debug:
            logging.debug ('')
            logging.debug (f'destruction= {self.destruction:s}')

        return (self.destruction)
#
#} end KoaJob.get_destruction
#
    
   
    def get_errorsummary (self):
#
#{ KoaJob.get_errorsummary
#

        if self.debug:
            logging.debug ('')
            logging.debug ('Enter get_errorsummary')

        if ((self.phase.lower() != 'error') and \
	    (self.phase.lower() != 'completed')):
        
            try:
                self.__get_statusjob ()

                if self.debug:
                    logging.debug ('')
                    logging.debug ('returned get_statusjob:')
                    logging.debug ('job= ')
                    logging.debug (self.job)

            except Exception as e:
           
                self.status = 'error'
       
	    
                if self.debug:
                    logging.debug ('')
                    logging.debug (f'exception: e= {str(e):s}')
                 
                raise Exception (self.msg)   
	
        if ((self.phase.lower() != 'error') and \
	    (self.phase.lower() != 'completed')):
        
            self.msg = 'The process is still running.'
            if self.debug:
                logging.debug ('')
                logging.debug (f'msg= {self.msg:s}')

            return (self.msg)
	
        elif (self.phase.lower() == 'completed'):
            
            self.msg = 'Process completed without error message.'
            
            if self.debug:
                logging.debug ('')
                logging.debug (f'msg= {self.msg:s}')

            return (self.msg)
        
        elif (self.phase.lower() == 'error'):

            self.errorsummary = self.job['uws:errorSummary']['uws:message']

            if self.debug:
                logging.debug ('')
                logging.debug (f'errorsummary= {self.errorsummary:s}')

            return (self.errorsummary)
#
#} end KoaJob.get_errorsummary
#
    
    
    def __get_statusjob (self):
#
#{ KoaJob.__get_statusjob
#

        if self.debug:
            logging.debug ('')
            logging.debug ('Enter __get_statusjob')
            logging.debug (f'statusurl= {self.statusurl:s}')

#
#   self.status doesn't exist, call get_status
#
        try:
            self.response = requests.get (self.statusurl, stream=True)
            
            if self.debug:
                logging.debug ('')
                logging.debug ('statusurl request sent')

        except Exception as e:
           
            self.msg = str(e)
	    
            if self.debug:
                logging.debug ('')
                logging.debug (f'exception: e= {str(e):s}')
            
            raise Exception (self.msg)    
     
        if self.debug:
            logging.debug ('')
            logging.debug ('response returned')
            logging.debug (f'status_code= {self.response.status_code:d}')

        if self.debug:
            logging.debug ('')
            logging.debug ('response.text= ')
            logging.debug (self.response.text)
        
        self.statusstruct = self.response.text

        if self.debug:
            logging.debug ('')
            logging.debug ('statusstruct= ')
            logging.debug (self.statusstruct)
        
#
#    parse returned status xml structure for parameters
#
        try:
            soup = bs.BeautifulSoup (self.statusstruct, 'xml')
       
            #soup = bs.BeautifulSoup (self.statusstruct, 'lxml')
       
        except Exception as e:

            self.msg = 'Failed to initialize BeautifulSoup: ' + str(e)
	    
            if self.debug:
                logging.debug ('')
                logging.debug (f'exception: e= {str(e):s}')
            
            raise Exception (self.msg)    
     

        if self.debug:
            logging.debug ('')
            logging.debug ('soup initialized')
       
#
#    get parameters from soup
#
        self.parameters = soup.find('uws:parameters')
        
        if self.debug:
            logging.debug ('')
            logging.debug ('self.parameters:')
            logging.debug (self.parameters)
        
#
#    convert status xml structure to dictionary doc 
#
        try:
            doc = xmltodict.parse (self.response.text)

        except Exception as e:

            self.msg = 'Failed to parse xmltodict: ' + str(e)

            if self.debug:
                logging.debug ('')
                logging.debug (f'exception: e= {str(e):s}')

            raise Exception (self.msg)

        if self.debug:
            logging.debug ('')
            logging.debug ('doc: ')
            logging.debug (doc)
        
#
#    check if this is a error message: in the structure of a votable
#
        votbl = None
        try: 
            votbl = doc['VOTABLE']
	
        except Exception as e:
           
            self.msg = 'Failed to extract votbl from doc '
	    
            if self.debug:
                logging.debug ('')
                logging.debug (f'exception: e= {str(e):s}')
            pass 
        
        if self.debug:
            logging.debug ('')
            logging.debug ('votbl found so it is an errmsg')
            logging.debug (votbl)

        
        if (votbl is not None):
        
            info = None
            infoval = ''
            errmsg = ''

            try: 
                info = votbl['RESOURCE']['INFO']

            except Exception as e:
           
                self.msg = 'Failed to extract INFO from doc '
	    
                if self.debug:
                    logging.debug ('')
                    logging.debug (f'exception: e= {str(e):s}')
            
                raise Exception (self.msg)    
     
            if self.debug:
                logging.debug ('')
                logging.debug ('info found: extract errmsg')
                logging.debug (info)

            errmsg = ''
            if (info is not None):
        
                try: 
                    infoval = info['@value'] 
                    errmsg = info['#text'] 
	
                except Exception as e:
           
                    self.msg = 'Failed to extract infoval and text from doc '
	    
                    if self.debug:
                        logging.debug ('')
                        logging.debug (f'exception: e= {str(e):s}')
            
                    raise Exception (self.msg)    
     
                if self.debug:
                    logging.debug ('')
                    logging.debug (f'infoval= {infoval:s}')
                    logging.debug (f'errmsg= {errmsg:s}')

                if (infoval.lower() == 'error'):
                    raise Exception (errmsg)    

#
# end votbl not None
#

        try: 
            self.job = doc['uws:job']
	
        except Exception as e:
           
            self.msg = 'Failed to extract uws:job from doc '
	    
            if self.debug:
                logging.debug ('')
                logging.debug (f'exception: e= {str(e):s}')
            
            raise Exception (self.msg)    
     
        if self.debug:
            logging.debug ('')
            logging.debug (f'self.job= ')
            logging.debug (self.job)


        self.phase = self.job['uws:phase']
        
        if self.debug:
            logging.debug ('')
            logging.debug (f'self.phase.lower():{ self.phase.lower():s}')
        
       
        if (self.phase.lower() == 'completed'):

            results = self.job['uws:results']
        
            if self.debug:
                logging.debug ('')
                logging.debug ('results')
                logging.debug (results)
            
            result = self.job['uws:results']['uws:result']
        
            if self.debug:
                logging.debug ('')
                logging.debug ('result')
                logging.debug (result)
            

            self.resulturl = \
                self.job['uws:results']['uws:result']['@xlink:href']
        
        elif (self.phase.lower() == 'error'):
            self.errorsummary = self.job['uws:errorSummary']['uws:message']


        if self.debug:
            logging.debug ('')
            logging.debug ('self.job:')
            logging.debug (self.job)
            logging.debug (f'self.phase.lower(): {self.phase.lower():s}')
            logging.debug (f'self.resulturl: {self.resulturl:s}')

        return
#
#} end KoaJob.__get_statusjob
#

#
#} end KoaJob class
#


Koa = Archive()
#print ('Koa instantiated')



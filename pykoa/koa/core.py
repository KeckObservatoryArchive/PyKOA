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
#import ijson
import xmltodict 
import tempfile
import bs4 as bs

import requests
import urllib 
import http.cookiejar

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

    Koa.query_datetime ('hires', '2018-03-16 00:00:00/2018-03-18 00:00:00', outpath= './meta.xml', format='ipac') 
    """
    
    tap = None

    parampath = ''
    outpath = ''
    format = 'ipac'
    maxrec = -1 
    query = ''
    
    content_type = ''
    outdir = ''
    astropytbl = None

    ndnloaded = 0
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

        if self.debug:
            logging.debug ('')
            logging.debug (f'baseurl= {self.baseurl:s}')

#
#    urls for nph-tap.py, nph-koaLogin, nph-makeQyery, 
#    nph-getKoa, and nph-getCaliblist
#
        self.tap_url = self.baseurl + '/TAP'
        self.login_url = self.baseurl + 'cgi-bin/KoaAPI/nph-koaLogin?'
        self.makequery_url = self.baseurl + 'cgi-bin/KoaAPI/nph-makeQuery?'
        self.caliblist_url = self.baseurl+ 'cgi-bin/KoaAPI/nph-getCaliblist?'
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

#
#    retrieve baseurl from conf class;
#
        if self.debug:
            logging.debug ('')
            logging.debug (f'conf.server= {conf.server:s}')

        self.baseurl = conf.server

        if self.debug:
            logging.debug ('')
            logging.debug (f'baseurl (from conf)= {self.baseurl:s}')

#
#  construct full url for login
#
        if ('server' in kwargs):
            self.baseurl = kwargs.get ('server')

        if self.debug:
            logging.debug ('')
            logging.debug (f'baseurl= {self.baseurl:s}')

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
        
        if self.debug:
            logging.debug ('')
            logging.debug ('')
            logging.debug ('Enter query_datetime:')
       
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

        if self.debug:
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
       
        if self.debug:
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
        
        if self.debug:
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

        if self.debug:
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
       
        if self.debug:
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
        
        if self.debug:
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
 
        if self.debug:
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
        
        if self.debug:
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

        if self.debug:
            logging.debug ('')
            logging.debug (f'instrument= {self.instrument:s}')
            logging.debug (f'object= {self.object:s}')
            logging.debug (f'outpath= {self.outpath:s}')

        radius = 0.5 
        if ('radius' in kwargs):
            radius_str = kwargs.get('radius')
            radius = float(radius_str)

        if self.debug:
            logging.debug ('')
            logging.debug (f'radius= {radius:f}')

        """
        coords = None
        try:
            print (f'resolving object name')
 
            coords = name_resolve.get_icrs_coordinates (object)
        
        except Exception as e:

            if self.debug:
                logging.debug ('')
                logging.debug (f'name_resolve error: {str(e):s}')
            
            print (str(e))
            return

        ra = coords.ra.value
        dec = coords.dec.value
        
        if self.debug:
            logging.debug ('')
            logging.debug (f'ra= {ra:f}')
            logging.debug (f'dec= {dec:f}')
        
        self.pos = 'circle ' + str(ra) + ' ' + str(dec) \
            + ' ' + str(radius)
	
        """

        lookup = None
        try:
            if self.debug:
                lookup = objLookup (object, debug=1)
            else:
                lookup = objLookup (object)
        
            if self.debug:
                logging.debug ('')
                logging.debug ('objLookup run successful and returned')
        
        except Exception as e:

            if self.debug:
                logging.debug ('')
                logging.debug (f'objLookup error: {str(e):s}')
            
            print (str(e))
            return 

        if (lookup.status == 'error'):
            
            self.msg = 'Input object [' + object + '] lookup error: ' + \
                lookup.msg
            
            print (self.msg)
            return

        if self.debug:
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
	
        if self.debug:
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
        
        if self.debug:
            logging.debug ('')
            logging.debug ('')
            logging.debug ('Enter query_criteria')
#
#    send url to server to construct the select statement
#
        self.outpath = outpath
 
        len_param = len(param)

        if self.debug:
            logging.debug ('')
            logging.debug (f'outpath= {self.outpath:s}')
            
            logging.debug ('')
            logging.debug (f'len_param= {len_param:d}')

            for k,v in param.items():
                logging.debug (f'k, v= {k:s}, {str(v):s}')

        self.cookiepath = ''
        if ('cookiepath' in kwargs): 
            self.cookiepath = kwargs.get('cookiepath')

        if self.debug:
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

        if self.debug:
            logging.debug ('')
            logging.debug (f'format= {self.format:s}')
            logging.debug (f'maxrec= {self.maxrec:d}')

        data = urllib.parse.urlencode (param)

#
#    retrieve baseurl from conf class;
#
#    during dev or test, baseurl will be a keyword input
#
        self.baseurl = conf.server

        if ('server' in kwargs):
            self.baseurl = kwargs.get ('server')

        if self.debug:
            logging.debug ('')
            logging.debug (f'baseurl= {self.baseurl:s}')

#
#    urls for nph-tap.py, nph-koaLogin, nph-makeQyery, 
#    nph-getKoa, and nph-getCaliblist
#
        self.tap_url = self.baseurl + 'TAP'
        self.makequery_url = self.baseurl + 'cgi-bin/KoaAPI/nph-makeQuery?'

        if self.debug:
            logging.debug ('')
            logging.debug (f'tap_url= [{self.tap_url:s}]')
            logging.debug (f'makequery_url= [{self.makequery_url:s}]')


        url = self.makequery_url + data            

        if self.debug:
            logging.debug ('')
            logging.debug (f'url= {url:s}')

        query = ''
        try:
            query = self.__make_query (url) 

            if self.debug:
                logging.debug ('')
                logging.debug ('returned __make_query')
  
        except Exception as e:

            if self.debug:
                logging.debug ('')
                logging.debug (f'Error: {str(e):s}')
            
            print (str(e))
            return 
        
        if self.debug:
            logging.debug ('')
            logging.debug (f'query= {query:s}')
       
        self.query = query

#
#    send tap query
#
        self.tap = None
        if (len(self.cookiepath) > 0):
            
            if self.debug:
                logging.debug ('')
                logging.debug ('xxx0')
                logging.debug (f'cookiepath= {self.cookiepath:s}')
       
            if self.debug:
                
                try:
                    self.tap = KoaTap (self.tap_url, \
                        format=self.format, \
                        maxrec=self.maxrec, \
                        cookiefile=self.cookiepath, \
	                debug=1)
                
                except Exception as e:
            
                    if self.debug:
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
            
                    if self.debug:
                        logging.debug ('')
                        logging.debug (f'Error: {str(e):s}')
                    
                    print (str(e))
                    return 
        
        else: 
            if self.debug:
                try:
                    self.tap = KoaTap (self.tap_url, \
                        format=self.format, \
                        maxrec=self.maxrec, \
	                debug=1)
                
                except Exception as e:
            
                    if self.debug:
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
            
                    if self.debug:
                        logging.debug ('')
                        logging.debug (f'Error: {str(e):s}')
                    
                    print (str(e))
                    return 
        
        if self.debug:
            logging.debug('')
            logging.debug('koaTap initialized')
            logging.debug('')
            logging.debug(f'query= {query:s}')

        print ('submitting request...')

        if self.debug:
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
        
        if self.debug:
            logging.debug ('')
            logging.debug (f'return self.tap.send_async:')
            logging.debug (f'retstr= {retstr:s}')

        retstr_lower = retstr.lower()

        indx = retstr_lower.find ('error')
    
#        if self.debug:
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
        
        if self.debug:
            logging.debug ('')
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
 
        if self.debug:
            logging.debug ('')
            logging.debug ('')
            logging.debug (f'query= {self.query:s}')
            logging.debug (f'outpath= {self.outpath:s}')
       
        self.cookiepath = '' 
        if ('cookiepath' in kwargs): 
            self.cookiepath = kwargs.get('cookiepath')

        if self.debug:
            logging.debug ('')
            logging.debug (f'cookiepath= {self.cookiepath:s}')

        self.format = 'ipac'
        if ('format' in kwargs): 
            self.format = kwargs.get('format')

        self.maxrec = -1 
        if ('maxrec' in kwargs): 
            self.maxrec = kwargs.get('maxrec')

        if self.debug:
            logging.debug ('')
            logging.debug (f'format= {self.format:s}')
            logging.debug (f'maxrec= {self.maxrec:d}')

#
#    retrieve baseurl from conf class;
#
        self.baseurl = conf.server

        if ('server' in kwargs):
            self.baseurl = kwargs.get ('server')

        if self.debug:
            logging.debug ('')
            logging.debug (f'baseurl= {self.baseurl:s}')

#
#    urls for nph-tap.py
#
        self.tap_url = self.baseurl + 'TAP'

        if self.debug:
            logging.debug ('')
            logging.debug (f'tap_url= [{self.tap_url:s}]')

#
#    send tap query
#
        self.tap = None

        if (len(self.cookiepath) > 0):
           
            if self.debug:
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
            if self.debug:
                self.tap = KoaTap (self.tap_url, \
                    format=self.format, \
                    maxrec=self.maxrec, \
	            debug=1)
            else:
                self.tap = KoaTap (self.tap_url, \
                    format=self.format, \
                    maxrec=self.maxrec)
        
        if self.debug:
            logging.debug('')
            logging.debug('koaTap initialized')
            logging.debug(f'query= {query:s}')
            logging.debug('call self.tap.send_async')

        print ('submitting request...')

        if self.debug:
            if (len(self.outpath) > 0):
                retstr = self.tap.send_async (query, \
                    outpath=self.outpath, \
                    format=self.format, \
                    maxrec=self.maxrec, \
                    debug=1)
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
        
        if self.debug:
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


    def print_data (self):
#
#{ Archive.print_date
#


        if self.debug:
            logging.debug ('')
            logging.debug ('Enter koa.print_data:')

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
        'download' method allows download of FITS files (and/or) 
        associated calibration files shown in their metadata file.

        *** Requirement: To download files, the following three columns: 
            instrume, koaid, and filehand must be included in the input
            metadata file.

	Required input:
	-----
	metapath (string): a full path metadata table obtained from running
	          query methods    
       
	format (string):   metadata table's format: ipac, votable, csv, or tsv.
	
        outdir (string):   the directory for depositing the returned files      
 
	
        Optional input:
        ----------------
        cookiepath (string): cookie file path for downloading the proprietary 
                             KOA data;
        
        start_row (integer): default is start_row = 0;
	
        end_row (integer): default is end_row = nrows - 1 where nrows is the 
                           number of rows in the metadata file;

        calibfile (integer): whether to download the associated calibration 
            files (0: do not download; 1: download);
            default is 0.
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
    
        if self.debug:
            logging.debug ('')
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

        self.metapath = metapath
        self.format = format
        self.outdir = outdir

        if self.debug:
            logging.debug ('')
            logging.debug (f'metapath= {self.metapath:s}')
            logging.debug (f'format= {self.format:s}')
            logging.debug (f'outdir= {self.outdir:s}')

        
        cookiepath = ''
        cookiejar = None
        
        if ('cookiepath' in kwargs): 
            cookiepath = kwargs.get('cookiepath')

        if self.debug:
            logging.debug ('')
            logging.debug (f'cookiepath= {cookiepath:s}')

        if (len(cookiepath) > 0):
   
            cookiejar = http.cookiejar.MozillaCookieJar (cookiepath)

            try: 
                cookiejar.load (ignore_discard=True, ignore_expires=True)
    
                if self.debug:
                    logging.debug (\
                        f'cookie loaded from file: {cookiepath:s}')
        
                for cookie in cookiejar:
                    
                    if self.debug:
                        logging.debug ('')
                        logging.debug ('cookie=')
                        logging.debug (cookie)
                        logging.debug (f'cookie.name= {cookie.name:s}')
                        logging.debug (f'cookie.value= {cookie.value:s}')
                        logging.debug (f'cookie.domain= {cookie.domain:s}')

            except Exception as e:
                if self.debug:
                    logging.debug ('')
                    logging.debug (f'loadCookie exception: {str(e):s}')
                pass

#        endif (cookiepath)

        fmt_astropy = self.format
        if (self.format == 'tsv'):
            fmt_astropy = 'ascii.tab'
        if (self.format == 'csv'):
            fmt_astropy = 'ascii.csv'
        if (self.format == 'ipac'):
            fmt_astropy = 'ascii.ipac'

#
#    read metadata to astropy table
#
        self.astropytbl = None
        try:
            self.astropytbl = Table.read (self.metapath, format=fmt_astropy)
        
        except Exception as e:
            self.msg = 'Failed to read metadata table to astropy table:' + \
                str(e) 
            print (self.msg)
            return
            #sys.exit()

        self.len_tbl = len(self.astropytbl)

        if self.debug:
            logging.debug ('')
            logging.debug ('self.astropytbl read')
            logging.debug (f'self.len_tbl= {self.len_tbl:d}')

        
        self.colnames = self.astropytbl.colnames

        if self.debug:
            logging.debug ('')
            logging.debug ('self.colnames:')
            logging.debug (self.colnames)
  
        self.len_col = len(self.colnames)

        if self.debug:
            logging.debug ('')
            logging.debug (f'self.len_col= {self.len_col:d}')

 
        self.ind_instrume = -1
        self.ind_koaid = -1
        self.ind_filehand = -1
        for i in range (0, self.len_col):

            if (self.colnames[i].lower() == 'instrume'):
                self.ind_instrume = i

            if (self.colnames[i].lower() == 'koaid'):
                self.ind_koaid = i

            if (self.colnames[i].lower() == 'filehand'):
                self.ind_filehand = i
             
        if self.debug:
            logging.debug ('')
            logging.debug (f'self.ind_instrume= {self.ind_instrume:d}')
            logging.debug (f'self.ind_koaid= {self.ind_koaid:d}')
            logging.debug (f'self.ind_filehand= {self.ind_filehand:d}')
      
        if (self.ind_instrume == -1):
            print ('Column [instrume] is required in the metadata file for downloading data.')
            return
            #sys.exit()
        
        if (self.ind_koaid == -1):
            print ('Column [koaid] is required in the metadata file for downloading data.')
            return
            #sys.exit()
        
        if (self.ind_filehand == -1):
            print ('Column [filehand] is required in the metadata file for downloading data.')
            return
            #sys.exit()
    
        if (self.len_tbl == 0):
            print ('There is no data in the metadata table.')
            return
            #sys.exit()
        
        calibfile = 0 
        if ('calibfile' in kwargs): 
            calibfile = kwargs.get('calibfile')
         
        if self.debug:
            logging.debug ('')
            logging.debug (f'calibfile= {calibfile:d}')

        srow = 0;
        erow = self.len_tbl - 1

        if ('start_row' in kwargs): 
            srow = kwargs.get('start_row')

        if self.debug:
            logging.debug ('')
            logging.debug (f'srow= {srow:d}')
     
        if ('end_row' in kwargs): 
            erow = kwargs.get('end_row')
        
        if self.debug:
            logging.debug ('')
            logging.debug (f'erow= {erow:d}')
     
        if (srow < 0):
            srow = 0 
        if (erow > self.len_tbl - 1):
            erow = self.len_tbl - 1 
 
        if self.debug:
            logging.debug ('')
            logging.debug (f'srow= {srow:d}')
            logging.debug (f'erow= {erow:d}')
     

#
#    create outdir if it doesn't exist
#
#    decimal mode work for both python2.7 and python3;
#
#    0755 also works for python 2.7 but not python3
#  
#    convert octal 0775 to decimal: 493 
#
        d1 = int ('0775', 8)

        if self.debug:
            logging.debug ('')
            logging.debug (f'd1= {d1:d}')
     
        try:
            os.makedirs (self.outdir, mode=d1, exist_ok=True) 

        except Exception as e:
            
            self.msg = f'Failed to create {self.outdir:s}: {str(e):s}'
            print (self.msg)
            return
            #sys.exit()

        if self.debug:
            logging.debug ('')
            logging.debug ('returned os.makedirs') 


#
#    retrieve baseurl from conf class;
#
        self.baseurl = conf.server

        if ('server' in kwargs):
            self.baseurl = kwargs.get ('server')

        if self.debug:
            logging.debug ('')
            logging.debug (f'baseurl= {self.baseurl:s}')

#
#    urls for nph-getKoa, and nph-getCaliblist
#
        self.getkoa_url = self.baseurl + 'cgi-bin/getKOA/nph-getKOA?return_mode=json&'
        self.caliblist_url = self.baseurl+ 'cgi-bin/KoaAPI/nph-getCaliblist?'

        if self.debug:
            logging.debug ('')
            logging.debug (f'self.getkoa_url= {self.getkoa_url:s}')
            logging.debug (f'self.caliblist_url= {self.caliblist_url:s}')


        instrument = '' 
        koaid = ''
        filehand = ''
        self.ndnloaded = 0
        self.ndnloaded_calib = 0
        self.ncaliblist = 0
      
        nfile = erow - srow + 1   
        
        print (f'Start downloading {nfile:d} FITS data you requested;')
        print (f'please check your outdir: {self.outdir:s} for  progress.')
 
        for l in range (srow, erow+1):
        #
        #{ for loop for download FITS files
        #
            if self.debug:
                logging.debug ('')
                logging.debug (f'l= {l:d}')
                logging.debug ('')
                logging.debug ('self.astropytbl[l]= ')
                logging.debug (self.astropytbl[l])
                logging.debug ('instrument= ')
                logging.debug (self.astropytbl[l][self.ind_instrume])

            instrument = self.astropytbl[l][self.ind_instrume]
            koaid = self.astropytbl[l][self.ind_koaid]
            filehand = self.astropytbl[l][self.ind_filehand]
	    
            if self.debug:
                logging.debug ('')
                logging.debug ('type(instrument)= ')
                logging.debug (type(instrument))
                logging.debug (type(instrument) is bytes)
            
            if (type (instrument) is bytes):
                
                if self.debug:
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
  
            if self.debug:
                logging.debug ('')
                logging.debug (f'l= {l:d} koaid= {koaid:s}')
                logging.debug (f'filehand= {filehand:s}')
                logging.debug (f'instrument= {instrument:s}')

#
#   get lev0 files
#
            url = self.getkoa_url + 'filehand=' + filehand
            filepath = self.outdir + '/' + koaid
                
            if self.debug:
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
                    self.ndnloaded = self.ndnloaded + 1

                    self.msg =  'Returned file written to: ' + filepath   
           
                    if self.debug:
                        logging.debug ('')
                        logging.debug ('returned __submit_request')
                        logging.debug (f'self.msg= {self.msg:s}')
            
                except Exception as e:
                    print (f'File [{koaid:s}] download: {str(e):s}')


            if (calibfile == 1):
            #
            # {   if calibfile == 1: download calibfile
            #
    
                if self.debug:
                    logging.debug ('')
                    logging.debug ('calibfile=1: downloading calibfiles')
	    
                koaid_base = '' 
                ind = -1
                ind = koaid.rfind ('.')
                if (ind > 0):
                    koaid_base = koaid[0:ind]
                else:
                    koaid_base = koaid

                if self.debug:
                    logging.debug ('')
                    logging.debug (f'koaid_base= {koaid_base:s}')
	    
                caliblist = self.outdir + '/' + koaid_base + '.caliblist.json'
                
                if self.debug:
                    logging.debug ('')
                    logging.debug (f'caliblist= {caliblist:s}')

                isExist = os.path.exists (caliblist)
	    
                if (not isExist):

                    if self.debug:
                        logging.debug ('')
                        logging.debug ('downloading calibfiles')
	    
                    url = self.caliblist_url \
                        + 'instrument=' + instrument \
                        + '&koaid=' + koaid

                    if self.debug:
                        logging.debug ('')
                        logging.debug (f'caliblist url= {url:s}')

                    try:
                        self.__submit_request (url, caliblist, cookiejar)
                        self.ncaliblist = self.ncaliblist + 1

                        self.msg =  'Returned file written to: ' + caliblist   
           
                        if self.debug:
                            logging.debug ('')
                            logging.debug ('returned __submit_request')
                            logging.debug (f'self.msg= {self.msg:s}')
            
                    except Exception as e:
                        #print (f'File [{caliblist:s}] download: {str(e):s}')
                        #self.msg = 'Error downloading caliblist [' + \
                        #    caliblist + ']:' + str(e)
                        
                        self.msg = 'No associated calibration list for ' + \
                            koaid
                        print (f'{self.msg:s}')
                        continue 
                         

#
#    check again after caliblist is successfully downloaded, if caliblist 
#    exists: download calibfiles
#     
                isExist = os.path.exists (caliblist)
                                  
                if (isExist):

                    if self.debug:
                        logging.debug ('')
                        logging.debug ('list exist: downloading calibfiles')
	    
                    try:
                        ncalibs = self.__download_calibfiles ( \
                            caliblist, cookiejar)
                        self.ndnloaded_calib = self.ndnloaded_calib + ncalibs
                
                        if self.debug:
                            logging.debug ('')
                            logging.debug ('returned __download_calibfiles')
                            logging.debug (f'{ncalibs:d} downloaded')

                    except Exception as e:
                
                        self.msg = 'Error downloading files in caliblist [' + \
                            filepath + ']: ' +  str(e)
                        
                        if self.debug:
                            logging.debug ('')
                            logging.debug (f'errmsg= {self.msg:s}')

#                endif (download_calibfiles):
            # 
            #} endif (calibfile == 1):
            #
        #
        #}        endfor l in range (srow, erow+1)
        #

        if self.debug:
            logging.debug ('')
            logging.debug (f'{self.len_tbl:d} files in the table;')
            logging.debug (f'{self.ndnloaded:d} files downloaded.')
            logging.debug (f'{self.ncaliblist:d} calibration list downloaded.')
            logging.debug (\
                f'{self.ndnloaded_calib:d} calibration files downloaded.')

        print (f'A total of new {self.ndnloaded:d} FITS files downloaded.')
 
        if (calibfile == 1):
            print (f'{self.ncaliblist:d} new calibration list downloaded.')
            print (f'{self.ndnloaded_calib:d} new calibration FITS files downloaded.')
        return
#
#} end Archive.download
#
    

    def __download_calibfiles (self, listpath, cookiejar):
#
#{ Archive.__download_calibfiles
#
    
        if self.debug:
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
        
            if self.debug:
                logging.debug ('')
                logging.debug (f'caliblist: {caliblist:s} load error')

            self.errmsg = 'Failed to read ' + listpath	
	
            fp.close() 
            
            raise Exception (self.errmsg)

            return

        nrec = len(data)
    
        if self.debug:
            logging.debug ('')
            logging.debug (f'downloadCalibfiles: nrec= {nrec:d}')

        if (nrec == 0):

            self.status = 'error'	
            self.errmsg = 'No data found in the caliblist: ' + listpath
	    
            raise Exception (self.errmsg)


#
#    retrieve koaid from caliblist json structure and download files
#
        ndnloaded = 0
        for ind in range (0, nrec):

            if self.debug:
                logging.debug (f'downloadCalibfiles: ind= {ind:d}')

            koaid = data[ind]['koaid']
            instrument = data[ind]['instrument']
            filehand = data[ind]['filehand']
            
            if self.debug:
                logging.debug (f'instrument= {instrument:s}')
                logging.debug (f'koaid= {koaid:s}')
                logging.debug (f'filehand= {filehand:s}')

#
#   get lev0 files
#
            url = self.getkoa_url + 'filehand=' + filehand
                
            filepath = self.outdir + '/' + koaid
                
            if self.debug:
                logging.debug ('')
                logging.debug (f'filepath= {filepath:s}')
                logging.debug (f'url= {url:s}')

#
#    if file exists, skip
#
            isExist = os.path.exists (filepath)
	    
            if (isExist):
                if self.debug:
                    logging.debug ('')
                    logging.debug (f'isExist: {isExist:d}: skip')
                     
                continue

            try:
                self.__submit_request (url, filepath, cookiejar)
                ndnloaded = ndnloaded + 1
                
                self.msg = 'calib file [' + filepath + '] downloaded.'

                if self.debug:
                    logging.debug ('')
                    logging.debug ('returned __submit_request')
                    logging.debug (f'self.msg: {self.msg:s}')
            
            except Exception as e:
                
                print (f'calib file download error: {str(e):s}')

        if self.debug:
            logging.debug ('')
            logging.debug (f'{self.ndnloaded:d} files downloaded.')

        return (ndnloaded)
#
#} end  Archive.__download_calibfiles
#
    

    def __submit_request(self, url, filepath, cookiejar):
#
#{ Archive.__submit_request
#

        if self.debug:
            logging.debug ('')
            logging.debug ('Enter database.__submit_request:')
            logging.debug (f'url= {url:s}')
            logging.debug (f'filepath= {filepath:s}')
       
            if not (cookiejar is None):  
            
                for cookie in cookiejar:
                    
                    if self.debug:
                        logging.debug ('')
                        logging.debug ('cookie saved:')
                        logging.debug (f'cookie.name= {cookie.name:s}')
                        logging.debug (f'cookie.value= {cookie.value:s}')
                        logging.debug (f'cookie.domain= {cookie.domain:s}')
            
        try:
            self.response =  requests.get (url, cookies=cookiejar, \
                stream=True)

            if self.debug:
                logging.debug ('')
                logging.debug ('request sent')
        
        except Exception as e:
            
            if self.debug:
                logging.debug ('')
                logging.debug (f'exception: {str(e):s}')

            self.status = 'error'
            self.msg = 'Failed to submit the request: ' + str(e)
	    
            raise Exception (self.msg)
            return
                       
        if self.debug:
            logging.debug ('')
            logging.debug ('status_code:')
            logging.debug (self.response.status_code)
      
      
        if (self.response.status_code == 200):
            self.status = 'ok'
            self.msg = ''
        else:
            self.status = 'error'
            self.msg = 'Failed to submit the request'
	    
            raise Exception (self.msg)
            return
                       
            
        if self.debug:
            logging.debug ('')
            logging.debug ('headers: ')
            logging.debug (self.response.headers)
      
      
        self.content_type = ''
        try:
            self.content_type = self.response.headers['Content-type']
        except Exception as e:

            if self.debug:
                logging.debug ('')
                logging.debug (f'exception extract content-type: {str(e):s}')

        if self.debug:
            logging.debug ('')
            logging.debug (f'content_type= {self.content_type:s}')


        if (self.content_type == 'application/json'):
            
            if self.debug:
                logging.debug ('')
                logging.debug (\
                    'return is a json structure: might be error message')
            
            jsondata = json.loads (self.response.text)
          
            if self.debug:
                logging.debug ('')
                logging.debug ('jsondata:')
                logging.debug (jsondata)

 
            self.status = ''
            try: 
                self.status = jsondata['status']
                
                if self.debug:
                    logging.debug ('')
                    logging.debug (f'self.status= {self.status:s}')

            except Exception as e:

                if self.debug:
                    logging.debug ('')
                    logging.debug (f'get status exception: e= {str(e):s}')

            self.msg = '' 
            try: 
                self.msg = jsondata['msg']
                
                if self.debug:
                    logging.debug ('')
                    logging.debug (f'self.msg= {self.msg:s}')

            except Exception as e:

                if self.debug:
                    logging.debug ('')
                    logging.debug (f'extract msg exception: e= {str(e):s}')

            errmsg = ''        
            try: 
                errmsg = jsondata['error']
                
                if self.debug:
                    logging.debug ('')
                    logging.debug (f'errmsg= {errmsg:s}')

                if (len(errmsg) > 0):
                    self.status = 'error'
                    self.msg = errmsg

            except Exception as e:

                if self.debug:
                    logging.debug ('')
                    logging.debug (f'get error exception: e= {str(e):s}')


            if self.debug:
                logging.debug ('')
                logging.debug (f'self.status= {self.status:s}')
                logging.debug (f'self.msg= {self.msg:s}')


            if (self.status == 'error'):
                raise Exception (self.msg)
                return

#
#    save to filepath
#
        if self.debug:
            logging.debug ('')
            logging.debug ('save_to_file:')
       
        try:
            with open (filepath, 'wb') as fd:

                for chunk in self.response.iter_content (chunk_size=1024):
                    fd.write (chunk)
            
            self.msg =  'Returned file written to: ' + filepath   
#            print (self.msg)
            
            if self.debug:
                logging.debug ('')
                logging.debug (self.msg)
	
        except Exception as e:

            if self.debug:
                logging.debug ('')
                logging.debug (f'exception: {str(e):s}')

            self.status = 'error'
            self.msg = 'Failed to save returned data to file: %s' % filepath
            
            raise Exception (self.msg)
            return

        return
#
#} end Archive.__submit_request
#
                       

    def __make_query (self, url):
#
#{ Archive.__make_query
#
       
        if self.debug:
            logging.debug ('')
            logging.debug ('Enter __make_query:')
            logging.debug (f'url= {url:s}')

        response = None
        try:
            response = requests.get (url, stream=True)

            if self.debug:
                logging.debug ('')
                logging.debug ('request sent')

        except Exception as e:
           
            msg = 'Error: ' + str(e)

            if self.debug:
                logging.debug ('')
                logging.debug (f'exception: e= {str(e):s}')
            
            raise Exception (msg)


        content_type = response.headers['content-type']

        if self.debug:
            logging.debug ('')
            logging.debug (f'content_type= {content_type:s}')
       
        if (content_type == 'application/json'):
                
            if self.debug:
                logging.debug ('')
                logging.debug (f'response.text: {response.text:s}')

#
#    error message
#
            try:
                jsondata = json.loads (response.text)
                 
                if self.debug:
                    logging.debug ('')
                    logging.debug ('jsondata loaded')
                
                status = jsondata['status']
                msg = jsondata['msg']
                
                if self.debug:
                    logging.debug ('')
                    logging.debug (f'status: {status:s}')
                    logging.debug (f'msg: {msg:s}')

            except Exception:
                msg = 'returned JSON object parse error'
                
                if self.debug:
                    logging.debug ('')
                    logging.debug ('JSON object parse error')
      
                
            raise Exception (msg)
            
            if self.debug:
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
        
            if self.debug:
                logging.debug ('')
                logging.debug ('xxx1')
       
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
            if self.debug:
                logging.debug ('')
                logging.debug ('xxx2')
       
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
            
            if debug:
                logging.debug ('')
                logging.debug (f'xxx1')
                
            self.msg = 'Result downloaded to file [' + self.outpath + ']'
        else:
#
#    read temp outpath to astropy table
#
            if debug:
                logging.debug ('')
                logging.debug (f'xxx2')
                
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
       
            for i in range (0, len_table):
	    
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
            soup = bs.BeautifulSoup (self.statusstruct, 'lxml')
       
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

            if self.debug:
                logging.debug ('')
                logging.debug ('xxx1: got here')
            
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



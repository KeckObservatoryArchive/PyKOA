"""
The pykoa package is KOA's (Keck Online Archive) python client interface for querying KOA's database and downloading KOA data.
"""
from astropy import config as _config


class Conf (_config.ConfigNamespace):
    
    """
    Configuration parameters for 'astroquery.koa'.
    """
    server = _config.ConfigItem (
        ['https://koa.ipac.caltech.edu/cgi-bin/'],
        'Name of the KOA server to use.') 

    timeout = _config.ConfigItem (
        60,
        'Time limit for connecting to KOA server.')


conf = Conf()

from .core import Koa, Archive, KoaTap, KoaJob

__all__ = ['Koa', 'Archive', 'KoaTap', 'KoaJob', 
           'Conf', 'conf',
           ] 

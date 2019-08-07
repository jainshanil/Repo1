from __future__ import unicode_literals
""" Config Manager for FSDlib.

    Provides namespaced configuration and parameters for 
    
"""
import sys, os
import re
import argparse
import posixpath

from itertools import ifilter,imap,chain
from collections import Mapping,defaultdict
# fnmatch does *-style filtering on filenames
from fnmatch import fnmatch

from fsd2.core.utils import CaseInsensitiveDict
from fsd2.core.localfs import LocalDirectory,LocalFile
from fsd2.core.config import ConfigFile,PARSE_FULL,PARSE_SIMPLE
from fsd2.logging import getChild,FsdLogger,setDebug,setInfo
cmlog = getChild(FsdLogger,"config.mgr")

from fsd2.core.utils import get_environment,get_fs_env

CM_SEP = "_" # namespaces for ConfigManager separated by underscores (for now)

CM_PATH = "/datalakebin/%s/fsd/consumption/conf/" % (get_fs_env())
CM_MASK = "CM_FSD_*.prm"
CM_FLOW_MASK = CM_MASK.replace("*","FLOW_*")
CM_GEN_MASK = CM_FLOW_MASK.replace("*","GEN_*")
CM_CLS = LocalDirectory
CM_OBJ_CLS = LocalFile

REGEX_SPLIT_LIST = re.compile("""(?:(?<!\\\)[,])""") # used for a re.split on list parsing

def get_flow_id(orig_filename):
    """ Utility method for processing a filename into a conformed flow ID."""
    if orig_filename:
        # Safety split to remove full path from filename (in case someone passes a HDFS or edge path in entirety)
        filename = posixpath.splitext(posixpath.split(orig_filename)[1])[0]
        #normname = "_".join(filter(lambda x:x,re.split('[ ._]',re.sub("""[0-9]{8}.*|[0-9]{6}_.*|(?i)WE[0-9]{6}.*""","",filename).upper())))
        normname = "_".join(filter(lambda x:x,re.split('[ ._]',re.sub("""[0-9]{8}.*|[0-9]{6}_.*|(?i)WE[0-9]{6}.*|(?i)D[0-9]{6}.*""","",filename).upper())))
        cmlog.debug("Processed filename '%s' to flow name '%s'" % (filename,normname))
        if not normname:
            # Returning blank values could be a big deal depending on how dependent components are written.
            # Thus we raise an Exception to alert support/dev to re-check their processes and troubleshoot the file(s).
            error_msg = "Flow ID normalized '%s' to a blank value ('%s'), please check filename and process for potential unexpected behavior." % (orig_filename,normname)
            cmlog.error(error_msg)
            raise Exception(error_msg)
        return normname

def parse_list(valstr):
    """ Used to return a list of values from a CM value."""
    vals = []
    if valstr:
        if isinstance(valstr,basestring):
            vals = [v.strip() for v in REGEX_SPLIT_LIST.split(valstr)]
        else:
            vals = list(valstr) # possibly not a str!
    return vals

def parse_bool(valstr):
    """ Used to return True or False from a CM value."""
    val = (unicode(valstr).strip().upper() == 'TRUE')
    return val

def resolve_key(cm_key_str):
    """ Takes a CM key and returns a value for it.
        CM keys are of the style:
        
        'cm:[config prefix:]config_key:section_key:data_key'
        for instance:
        'cm:flow:ret_prem_gl_extrct:flow_smithraw:flow_db'
        'cm:parse directive:<yourfilename>:flow_smithraw:flow_db'
        
        Strings are always returned for a given CM key.
    """
    cm_key_str = cm_key_str.lower()
    cm_key = cm_key_str.split(":")
    cmlog.debug("Processing CM shortcut key as [%s]" % (", ".join(cm_key)))
    cm_key.reverse()
    # check to ensure it's a CM key
    cm_prefix = cm_key.pop()
    if cm_prefix != 'cm':
        raise ValueError("Key '' provided is not a valid ConfigManager shortcut key.")
    cm_obj = None
    cm_kw = cm_key.pop()
    # check if we're getting a specific directive ('flow' or 'filename')
    if cm_kw == 'flow':
        flow_kw = cm_key.pop().upper()
        cmlog.debug("Key specifies flow ID '%s'" % (flow_kw))
        cm_obj = ConfigManager(flow=flow_kw)
    elif cm_kw == 'filename':
        file_kw = cm_key.pop()
        cmlog.debug("Key specifies file '%s'" % (file_kw))
        cm_obj = ConfigManager(filename=file_kw)
    else:
        cmlog.debug("Loading CM object '%s' directly (namespace processing will be BYPASSED)" % (cm_kw))
        cm_obj = ConfigManager(cm_kw)
    cm_val = cm_obj[cm_key[1],cm_key[0]]
    return cm_val

class ConfigError(Exception):
    pass

class ConfigNotFound(ConfigError):
    pass

class ConfigManager(Mapping):
    """ 
        Provides namespace based access to configuration files 
        and session-based access to stored state. 
    """
    def __init__(self,*args,**kwargs):
        """ Sets up the CM object and reads in the requested configs."""
        req_namespaces = args # the explicitly requested namespaces - considered MANDATORY to load successfully
        cmlog.debug("ConfigManager object loading with %d specified namespaces (%s)" % (len(req_namespaces),",".join(req_namespaces)))
        
        # instantiate the config directory
        self.conf_dir = CM_CLS(CM_PATH)
        
        # load explicitly requested namespaces first and throw if not found
        self._configfiles = self.get_configs(req_namespaces,required=True)
        # self.confs is usually a CaseInsensitiveDict
        self.confs = self.process_configs(self._configfiles)
        
        # check if a file or flow was provided explicitly
        req_flow = kwargs.get('flow',None)
        if not req_flow: # sometimes we pass {'flow':None} depending on calling approach
            req_flow = get_flow_id(kwargs.get('filename',None))
        spec_flow = 0
        if req_flow:
            spec_flow = 1
            flow_conf = self.get_config_by_flow(req_flow)
            flow_dict = self.process_genconf(flow_conf,req_flow)
            self.confs.update(flow_dict)
        
        cmlog.info("ConfigManager object loaded %d configurations." % (len(self._configfiles)+spec_flow))
    
    def expand_namespaces(self,namespaces):
        """ Parses and simplifies all referenced namespaces, de-duplicates etc."""
        raise NotImplementedError("'expand_namespaces' should not be used.")
        final_namespaces = set()
        for namespace in namespaces:
            # Split into its components and add to final_namespaces in order
            # eg: CM_FSD_FLOW_MTMEDSUP -> CM, CM_FSD, CM_FSD_FLOW, CM_FSD_FLOW_MEDSUP
            namesplit = namespace.split(CM_SEP)
            namesplit_n = len(namesplit)
            while namesplit_n > 0:
                # join decrementing number of elements in split namespace per loop
                final_namespaces.add(CM_SEP.join(namesplit[:namesplit_n]))
                namesplit_n -= 1 # decrement the slicer namesplit_n
        final_namespaces = final_namespaces.difference(set(namespaces))
        cmlog.debug("Namespaces provided: [%s] resolved to additional namespace combinations: [%s]" % (",".join(namespaces),",".join(final_namespaces)))
        return list(final_namespaces)
    
    def make_conf_path(self,namespace):
        """ Standard method for making a path to a CM file."""
        conf_name = "%s.prm" % (namespace)
        conf_path = os.path.join(CM_PATH,conf_name)
        return conf_path
    
    def process_genconf(self,conf_file,flow_id):
        """ Processes a genconf file (optionally with namespaced sections) to 
            be a standad flat dict to add to other conf dicts. """
        # there are a lot of commented cmlog.debug() calls in here...they're extras for troubleshooting namespaces, as needed.
        headers = defaultdict(set) # {header:set(namespace1,namespace2)}
        flow_id = flow_id.lower() # lower since everything's lowercase or case insensitive and flow_id normalizes to LOTS_OF_CAPS_EVERYWHERE
        
        # enumerate headers with namespaces
        for fk in conf_file.headers:
            splithdr = fk.split("::")
            #cmlog.debug("Split header '%s' into components '%s'" % (fk,splithdr))
            k = splithdr[0]
            ns = splithdr[1:]
            if ns and ns[0].lower()==flow_id:
                headers[k].add(ns[0])
            elif not ns:
                headers[k].add('')
        
        #cmlog.debug("Headers map for genconf file: %s" % (headers))
        # get the 'base' fields from the genconf file
        fields_base = defaultdict(dict,((k,dict(conf_file[k])) for k,nset in headers.iteritems() if '' in nset))
        #cmlog.debug("Base fields: %s" % (fields_base))
        
        # allows us to use explicitly what key is provided in the header key, rather than searching for 'in' nset
        for k in headers:
            headers[k].discard('')
        
        # now add in the flow's fields (overwriting base where conflicts happen)
        for k,nset in headers.iteritems():
            #cmlog.debug("Processing namespaces for header '%s' (%s)..." % (k,",".join(nset)))
            if nset:
                ns_flow = nset.pop()
                ns_key = "%s::%s" % (k,ns_flow)
                ns_dict = dict(conf_file[ns_key])
                #cmlog.debug("Updating base fields with: '%s'" % (ns_dict))
                fields_base[k].update(ns_dict)
                
        flat_genconf = CaseInsensitiveDict(dict(chain(*[[((k,sk),sv) for sk,sv in sd.iteritems()] for k,sd in fields_base.iteritems()])))
        
        return flat_genconf
    
    def process_configs(self,confs_dict):
        """ Does the 'stacking' of the configurations."""
        fullconf = CaseInsensitiveDict({})
        confkeys = sorted(confs_dict.iterkeys()) # do a lexicographic sort on the namespaces so we get cascading configs
        for k in confkeys:
            # iterate through the various config files we've loaded
            if confs_dict[k].parsestyle == PARSE_SIMPLE:
                # amend all keys to be 2-tuples instad of strings
                interim_dict = CaseInsensitiveDict(dict((('',k),v) for k,v in confs_dict[k].iteritems()))
                fullconf.update(interim_dict)
            else:
                fullconf.update(CaseInsensitiveDict(dict(((a,b),c) for (a,b),c in confs_dict[k].iteritems())))
        return fullconf
    
    def get_config_by_flow(self,flow_id):
        """ Attempts to retrieve a configuration file based on the flow ID.
            1. Attempts to retrieve file by default method
            2. If that fails, it lists out the gen-conf files and checks FOR_FLOW attributes.
        """
        flow_id = flow_id.upper()
        # Generates the conforming namespace
        namespace = CM_FLOW_MASK.replace("*",flow_id).replace(".prm","")
        cmlog.debug("Rewrote flow_id '%s' as conforming namespace '%s'." % (flow_id,namespace))
        # Attempts to retrieve the flow's direct-named conf file
        conf_file = self.get_configs([namespace],required=False)
        
        if conf_file:
            conf_file = conf_file.values()[0]
            cmlog.debug("Found configuration file '%s' for handling flow_id '%s'." % (conf_file.path,flow_id))
        else:
            # Search for a gen-conf that handles this flow
            genconf_filter = lambda cfg: flow_id in (flow_name.upper().strip() for flow_name in cfg.get(("flow_setup","for_flows"),"").split(","))
            all_genconf = list(ifilter(genconf_filter,self.list_configs(self.list_config_files(mask=CM_GEN_MASK))))
            # if one result, mission accomplished
            if len(all_genconf) == 1:
                conf_file = all_genconf[0]
                cmlog.info("Genconf file '%s' provides configuration for flow ID '%s'" % (conf_file.file.path,flow_id))
            # if no result, explode.
            elif not all_genconf:
                err_msg = "No configuration file found for handling flow_id '%s'." % (flow_id)
                cmlog.error(err_msg)
                raise ConfigNotFound(err_msg)
            # if >1 result, explode differently (ambiguous config)
            elif len(all_genconf) > 1:
                err_msg = "More than one configuration file was found for handling flow_id '%s' (%s)" % (flow_id,",".join(cfg.file.path for cfg in all_genconf))
                cmlog.error(err_msg)
                raise ConfigError(err_msg)
            
        return conf_file

    def get_configs(self,namespaces,required=False):
        """ Reads in requested configs by CM namespace.
            required = True will throw ConfigNotFound if a CM file isn't found for all values of namespaces
        """
        rtn_confs = {}
        for namespace in namespaces:
            # Iterates through the namespaces requested and tries to instantiate a ConfigFile
            namespace = namespace.upper()
            conf_pth = self.make_conf_path(namespace)
            try:
                conf = ConfigFile(conf_pth,checkexists=required)
                rtn_confs[namespace] = conf
            except Exception as e:
                if required:
                    # re-raise the exception
                    err_msg = "Problem loading configuration file for namespace '%s' at path '%s'." %(namespace,conf_pth)
                    cmlog.error(err_msg)
                    raise ConfigNotFound(err_msg)
                else:
                    log_msg = "Configuration file for namespace '%s' not found at path '%s'." %(namespace,conf_pth)
                    cmlog.info(log_msg)
        return rtn_confs
    
    def list_configs(self,files):
        """ Yields ConfigFile entries if they parse successfully, else logs the failure as a WARN."""
        for f in files:
            try:
                cf = ConfigFile(f.path)
                yield cf
            except Exception, e:
                cmlog.warn("Failed parsing presumed CM file at location '%s'" % (f.path))
                cmlog.warn("Parsing failed with exception %s" % (unicode(e)))
    
    def list_config_files(self,mask=CM_MASK):
        """ Lists mask-matching configuration files found in the CM_PATH.
            Does NOT search the CM_PATH recursively - see recursive=False arg below. """
        # Retrieve a list of matching configuration files (BaseFile objects) matching the file mask 'mask'
        matching_configs = list(ifilter(lambda f: fnmatch(posixpath.split(f.path)[1],mask),self.conf_dir.iter_contents(files=True,dirs=False,recursive=False)))
        cmlog.debug("Found %d files matching configuration mask '%s' in '%s'." % (len(matching_configs),mask,self.conf_dir.path))
        return matching_configs
    
    def __getitem__(self,key):
        """ Provides a cmobject[key] lookup to pretend its a dictionary.
            Accepts a single string 'abc', two strings 'abc','def' or a tuple ('abc','def')
        """
        key_str = isinstance(key,basestring)
        if key_str:
            # If you pass one key in, we give you a dict
            rtndict = CaseInsensitiveDict(dict((w,self.confs[v,w]) for v,w in self.confs.iterkeys() if v==key.lower()))
            if rtndict:
                return rtndict
            else:
                raise KeyError("Key '%s' not present" % (key))
        else:
            # If you pass two keys in, we give you back a value
            key = tuple(k for k in key)
            return self.confs.__getitem__(key)
    
    def __iter__(self):
        """ Iterates over the processed/stacked configuration."""
        return iter(self.confs)
    
    def __len__(self):
        return len(self.confs)
    
    @property
    def sections(self):
        return sorted(set(x[0] for x in self.iterkeys()))
    
    @property
    def files(self):
        """ Returns paths used by this CM object."""
        paths = list(set(c.path for c in self._configfiles))
        return paths
    
    def print_config(self):
        """ Pretty-prints the resolved configs. """
        sections = self.sections
        max_header = max(len(s) for s in sections)+3
        max_keys = max(len(k[1]) for k in self.iterkeys())+3
        for s in sections:
            sitems = sorted(self[s].items()) # caseinsensitivedict
            # print first line, which is header, with following first key
            print s.ljust(max_header),sitems[0][0].ljust(max_keys),"\t",sitems[0][1]
            for sitem in sitems[1:]:
                print " ".ljust(max_header),sitem[0].ljust(max_keys),"\t",sitem[1]
            print ""
    
    def new_session_config(self,sessionid=None,headers=[]):
        """ Returns a new MutableConfig for a temp configuration file."""
        raise NotImplementedError()
    
    def get_session_config(self,sessionid=None):
        """ Retrieves an existing session config by session ID."""
        raise NotImplementedError()
    

def get_parser():
    """ Sets up command line argument handling """
    parser = argparse.ArgumentParser(description="Command line ConfigMgr data dump.")
    
    # @README **add** parser.add_argument() calls as necessary - https://docs.python.org/2/library/argparse.html#the-add-argument-method
    
    parser.add_argument("--debug",action="store_true",help="enables debug-level logging")
    try:
        parse_args = parser.parse_args()
        return parse_args
    except:
        print " "
        sys.exit(1)

# Config Manager when run from commandline will provide debug information for reading config files
if __name__== '__main__':
    # Get command line arguments from argparse
    args = get_parser()
    if args.debug:
        setDebug()
    else:
        setInfo()
    
    cmlog.info("Beginning ConfigManager debug session. Root CM_PATH: '%s'" % (CM_PATH))
    try:
        # 1. Establish that the CM_PATH directory exists
        cm_dir = CM_CLS(CM_PATH)
        cmlog.info("CM_PATH exists: %s" % (cm_dir.exists))
        # 2. Enumerate how many files are in the directory itself (non-recursively) and how many match the CM file pattern (CM_*.prm)
        cm_dir_files = cm_dir.files
        cm_dir_confs = ConfigManager().list_config_files()
        cmlog.info("CM_PATH contains %d files, %d may be CM config files." % (len(cm_dir_files),len(cm_dir_confs)))
        cmlog.info("Configuration files found in CM_PATH root directory: \n%s" % ("\n".join("\t%3d. %s" % (i,f.path) for i,f in enumerate(cm_dir_confs,start=1))))
        # 3. Attempt to parse each one and determine its type and number of headers + keys
        cmlog.info("Attempting to parse found CM config files.")
        for i,f in enumerate(cm_dir_confs,start=1):
            try:
                conf = ConfigFile(f.path)
                conf_style = 'full' if conf.parsestyle == PARSE_FULL else 'simple'
                cnt_headers = len(conf.headers)
                cnt_keys = len(conf)
                cmlog.info("%3d. Config file '%s' (%s style) - %d headers, %d keys" % (i,f.path,conf_style,cnt_headers,cnt_keys))
                
            except Exception, e:
                cmlog.error("%3d. Failed parsing CM file '%s' with error '%s'" % (i,f.path,e))
        
        
    except Exception, e:
        # Catch-all for errors not caught elsewhere
        error_msg = "Script failed with error '%s'." % (unicode(e))
        cmlog.error(error_msg)
        exc_info = sys.exc_info()
        raise exc_info[1],None,exc_info[2]
        sys.exit(1)
    cmlog.info("Debug session completed successfully.")

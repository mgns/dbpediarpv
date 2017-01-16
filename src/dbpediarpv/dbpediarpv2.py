#!/usr/bin/env python
"""
Create a file for each resource for a set of DBpedia versions from the dumps in order to diff the resources.

Versions for processing are configured in datasets.json

$Id$
"""

import json
import os, errno
from os import listdir
import bz2
import urllib
#import urllib.request
import re
import shutil
import codecs
import concurrent.futures
import sys, getopt
from collections import defaultdict
print(sys.path)

from elasticsearch import Elasticsearch

import datetime
import logging
logger = logging.getLogger('dbpediarpv.' + __name__)
logging.basicConfig(filename='process.log', format='%(asctime)s %(levelname)s:%(message)s', level=logging.INFO)
#logging.basicConfig(format='%(asctime)s %(levelname)s:%(message)s', level=logging.DEBUG)

config_file = "datasets.json"
#destpath = "/tmp/dbpediarpv/"
#srcpath = "/Users/magnus/Datasets/DBpedia/"
destpath = "/home/magnus/Data/dbpediarpv2/"
srcpath = "/home/magnus/Data/dbpedia/"
clearDest = True
prefixes = {
	"cyc":		"http://sw.cyc.com/concept/",
	"d0":		"http://www.ontologydesignpatterns.org/ont/d0.owl#",
	"dbc":		"http://dbpedia.org/resource/Category:",
	"dbo":		"http://dbpedia.org/ontology/",
	"dbp":		"http://dbpedia.org/property/",
	"dbr":		"http://dbpedia.org/resource/",
	"dc":		"http://purl.org/dc/terms/",
	"dc11":		"http://purl.org/dc/elements/1.1/",
	"fbase":	"http://rdf.freebase.com/ns/",
	"foaf":		"http://xmlns.com/foaf/0.1/",
	"geo":		"http://www.w3.org/2003/01/geo/wgs84_pos#",
	"georss":	"http://www.georss.org/georss/",
	"owl":		"http://www.w3.org/2002/07/owl#",
	"prov":		"http://www.w3.org/ns/prov#",
	"rdf":		"http://www.w3.org/1999/02/22-rdf-syntax-ns#",
	"rdfs":		"http://www.w3.org/2000/01/rdf-schema#",
	"schema":	"http://schema.org/",
	"skos":		"http://www.w3.org/2004/02/skos/core#",
	"umb":		"http://umbel.org/umbel/rc/",
	"wd":		"http://wikidata.dbpedia.org/resource/",
	"wco":		"http://commons.wikimedia.org/wiki/",
	"wmc":		"http://upload.wikimedia.org/wikipedia/commons/",
	"wmen":		"http://upload.wikimedia.org/wikipedia/en/",
	"wp":		"http://en.wikipedia.org/wiki/",
	"yago":		"http://dbpedia.org/class/yago/"
}
prefixesRev = {v: k for k, v in prefixes.items()}

# Create regular expressions from the prefixes
isResourceRegex = re.compile("^(%s:[^>]*|<[^>]*>)" % "|".join(map(re.escape, prefixes.keys())))
# alternatives ("|") are tried from left to right, so they should be ordered properly to match correctly
# order by prefix, so alphabetically lower prefixes are exchanged with priority, e.g. dbc before dbr
prefixifyRegex = re.compile("^<(%s)" % "|".join(map(re.escape, sorted(prefixesRev, key=prefixesRev.get))))
unPrefixifyRegex = re.compile("^(%s):" % "|".join(map(re.escape, prefixes.keys())))

def main(argv):
	#logging.info("Hello.")
	
#	print("<http://dbpedia.org/resource/%21G%C3%A3%21ne_language>")
#	print(prefixify("<http://dbpedia.org/resource/%21G%C3%A3%21ne_language>"))
#	print(unPrefixify(prefixify("<http://dbpedia.org/resource/%21G%C3%A3%21ne_language>")))
#	print(getResourcePath(asDecodedDBpediaUrl(prefixify("<http://dbpedia.org/resource/Articles_for_creation/2006-10-21>"))[4:], destpath))
	
	#print(isResourceRegex)
	#print(prefixifyRegex)
	#print(unPrefixifyRegex)

	try:
		opts, args = getopt.getopt(argv,"",["process","compare"])
	except getopt.GetoptError:
		print('usage: dbpediarpv.py [--process|--compare]')
		sys.exit(2)
	for opt, arg in opts:
		if opt == '--process':
			versions = readVersions()
			#processVersions(versions)
			processVersionsParallel(versions)
			sys.exit()
		elif opt == '--compare':
			versions = readVersions()
			compareVersions(versions)
			sys.exit()
		
def readVersions():
	versions = []
	with open(config_file) as jfile:
		versions = json.load(jfile)
	#versions = json.loads('{ "order": ["1.0","2.0"],\n "version": [ { "id": "1.0", "dir": "1.0/", "files": ["test.nt"] },\n { "id": "2.0", "dir": "2.0/", "files": [ "test.nt.bz2" ] } ] }')
	return versions

def processVersions(versions):
	for v in versions["version"]:
		processVersion(v)

def processVersionsParallel(versions):
	with concurrent.futures.ProcessPoolExecutor() as executor:
	#with concurrent.futures.ThreadPoolExecutor(max_workers=4) as executor:
		logging.info("#### Start " + str(len(versions["version"])) + " processes in parallel ####")
		
		futures = executor.map(processVersion, versions["version"])
		for future in futures:
			logging.info(format(future))

def processVersion(version):
	logging.info("### DBpedia " + version["id"] + ": " + str(len(version["files"])) + " files ###")
	vdestpath = os.path.join(destpath, version["id"])
	if (clearDest):
		rmdir(vdestpath)
	mkdir(vdestpath)
	rfile = openA(os.path.join(vdestpath, 'nodbp'))
	for file in version["files"]:
		try:
			processFile(os.path.join(srcpath, version["dir"], file), vdestpath, rfile)
		except BaseException as e:
			logging.error("--- Error while processing " + os.path.join(srcpath, version["dir"], file) + " : " + str(e))
			pass
	rfile.close()
	logging.info("### DBpedia " + version["id"] + " done : " + str(len(version["files"])) + " files ###")
	return vdestpath

def processFile(fsrcpath, vdestpath, rfile):
	if (not os.path.isfile(fsrcpath)):
		logging.error("--- File not found: " + fsrcpath)
		return
	logging.info("## Process file " + fsrcpath + " ##")
	srcfile = decompress(fsrcpath)
	if (srcfile is None):
		logging.error("--- File not decompressable: " + fsrcpath)
		return
	filecontents = defaultdict(set)
	lines = srcfile.readlines()
	maxlines = -1000
	i = 0
	for line in lines:
		logging.debug(line)
		try:
			line = line.strip()
			try:
				line = line.decode(encoding='unicode_escape', errors='ignore')
			except AttributeError as e:
				line = line.encode('utf-8').decode(encoding='unicode_escape', errors='ignore')
		
			# ignore empty and commented lines
			if (line.strip() and line.strip()[0] != '#'):
				f, content = processTriple(line, vdestpath, rfile)
				if (not f is None):
					filecontents[f].add(content)
		except BaseException as e:
			logging.error("--- Error while processing " + fsrcpath + " line: " + str(i) + " / " + line + " : " + str(e))
			pass
		if (maxlines > 0 and i > maxlines):
			break
		if (len(filecontents.keys()) > 0 and i > 0 and i % 10000 == 0):
			logging.info("-- Write contents of file " + fsrcpath + " having " + str(len(filecontents.keys())) + " entries --")
			for f, content in filecontents.items():
				logging.debug("+++ Write " + f + "\n + " + "\n + ".join(content))
				sfile = openA(f)
				sfile.write("\n".join(content) + "\n")
				sfile.close()
			filecontents.clear()
		i = i + 1

	logging.info("## Write contents of file " + fsrcpath + " having " + str(len(filecontents.keys())) + " entries ##")
	for f, content in filecontents.items():
		logging.debug("+++ Write " + f + "\n + " + "\n + ".join(content))
		sfile = openA(f)
		sfile.write("\n".join(content) + "\n")
		sfile.close()

	logfile = openA(os.path.join(vdestpath, "processed.log"))
	st = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
	logfile.write(st + " " + fsrcpath + "\n")
	logging.info("## Processed file " + fsrcpath + " ##")

def processTriple(triple, path, rfile):
	logging.debug("+++ Triple: " + triple)
	# remove trailing dot and split triple into s p o
	spo = triple.strip(' .').split()
	s, p , o = spo[0], spo[1], " ".join(spo[2:])
	logging.debug("+++ SPO = " + s + " " + p + " " + o)
	logging.debug("+++ SPO prefixed = " + prefixify(s) + " " + prefixify(p) + " " + prefixify(o))
	
	p_s = prefixify(s)
	p_p = prefixify(p)
	
	if (isDBpediaResource(s)):
		db_s = asDBpediaUrl(p_s)
		logging.debug("+++ DBpedia resource: " + db_s.format())
		#logging.debug(spath)
		#appendToDBpediaResource(db_s, p_p, o, path)
		
		spath = getResourcePath(db_s, path)
		if (isResource(o)):
			p_o = prefixify(o)
			db_o = asDBpediaUrl(p_o)
			logging.debug(p_p + " " + db_o)
			return spath, p_p + " " + db_o
		else:
			return spath, p_p + " " + o
	else:
		logging.debug("--- No DBpedia resource: " + p_s)
		#appendToNoDBpediaResource(p_s, p_p, o, rfile)
		return None, None

def appendToDBpediaResource(s, p, o, path):
	logging.debug(s)
	spath = getResourcePath(s, path)
	logging.debug(spath)
	sfile = openA(spath)
	if (isResource(o)):
		p_o = prefixify(o)
		db_o = asDBpediaUrl(p_o)
		#sfile.write(s + " " + p + " " + db_o + "\n")
		logging.debug(p + " " + db_o)
		sfile.write(p + " " + db_o + "\n")
	else:
		#sfile.write(db_s + " " + prefixify(p) + " " + o + "\n")
		logging.debug(p + " " + o)
		sfile.write(p + " " + o + "\n")
	sfile.close()

def appendToNoDBpediaResource(s, p, o, rfile):
	#rpath = path + "noDBpResource"
	#rfile = openA(rpath)
	if (isResource(o)):
		p_o = prefixify(o)
		db_o = asDBpediaUrl(p_o)
		rfile.write(s + " " + p + " " + db_o + "\n")
	else:
		rfile.write(s + " " + p + " " + o + "\n")
	#rfile.close()

def compareVersions(versions):
	order = versions['order']
	logging.debug("Order:" + str(order))
	for idx, val in enumerate(order[:-1]):
		v1 = val
		v2 = order[idx+1]
		logging.info("Compare " + v1 + " -> " + v2)
		v1dir = os.path.join(destpath, v1, 'dbr')
		v2dir = os.path.join(destpath, v2, 'dbr')
		cD = compareDirs(v1dir, v2dir)
		logging.info('## Added    : ' + str(len(cD['added'])) + ' resources ##')
		logging.info('## Remained : ' + str(len(cD['remained'])) + ' resources ##')
		logging.info('## Removed  : ' + str(len(cD['removed'])) + ' resources ##')
		
		with open(os.path.join(destpath, v2, 'added'), 'w') as f:
			f.write("\n".join(cD['added']))
		with open(os.path.join(destpath, v2, 'remained'), 'w') as f:
			f.write("\n".join(cD['remained']))
		with open(os.path.join(destpath, v2, 'removed'), 'w') as f:
			f.write("\n".join(cD['removed']))

def compareDirs(dir1, dir2):
	added = []
	remained = []
	removed = []

	dir1list = [ d for d in listdir(dir1) if os.path.isdir(os.path.join(dir1, d)) ]
	dir2list = [ d for d in listdir(dir2) if os.path.isdir(os.path.join(dir2, d)) ]

	for d in dir1list:
		if (d in dir2list):
			cF = compareFiles(os.path.join(dir1, d), os.path.join(dir2, d))
			logging.debug(d + ' added    : ' + str(len(cF['added'])) + ' resources')
			logging.debug(d + ' remained : ' + str(len(cF['remained'])) + ' resources')
			logging.debug(d + ' removed  : ' + str(len(cF['removed'])) + ' resources')
			added.extend(cF['added'])
			remained.extend(cF['remained'])
			removed.extend(cF['removed'])
		else:
			f1list = [ f for f in listdir(os.path.join(dir1, d)) if os.path.isfile(os.path.join(dir1, d, f)) ]
			removed.extend(f1list)
	for d in dir2list:
		if (not d in dir1list):
			f2list = [ f for f in listdir(os.path.join(dir2, d)) if os.path.isfile(os.path.join(dir2, d, f)) ]
			added.extend(f2list)
	return {'added' : added, 'remained' : remained, 'removed' : removed}

def compareFiles(dir1, dir2):
	added = []
	remained = []
	removed = []

	dir1list = [ f for f in listdir(dir1) if os.path.isfile(os.path.join(dir1, f)) ]
	dir2list = [ f for f in listdir(dir2) if os.path.isfile(os.path.join(dir2, f)) ]

	for f in dir1list:
		if (f in dir2list):
			remained.append(f)
		else:
			removed.append(f)
	for f in dir2list:
		if (not f in dir1list):
			added.append(f)
	return {'added' : added, 'remained' : remained, 'removed' : removed}

def isResource(s):
	return re.match(isResourceRegex, s)

def isDBpediaResource(s):
	if (len(s) > 256):
		return False
	return re.match(r"^((dbr|dbc|dbo|dbp):[^>]*|<http://dbpedia\.org/resource/[^>]*>)", s)

def prefixify(s):
	if (re.match(prefixifyRegex, s)):
  		# For each match, look-up corresponding value, start shifted by 1 because of "<"
		s = prefixifyRegex.sub(lambda mo: prefixesRev[mo.string[mo.start()+1:mo.end()]] + ":", s)
		s = s.strip("<>")
	return s

def unPrefixify(s):
	if (re.match(unPrefixifyRegex, s)):
  		# For each match, look-up corresponding value, end shifted by 1 because of ":"
		s = unPrefixifyRegex.sub(lambda mo: prefixes[mo.string[mo.start():mo.end()-1]], s)
		s = "<" + s + ">"
	return s

def asDBpediaUrl(s):
	if (isDBpediaResource(s)):
		s = re.sub(r"[/]", "-", s)
	return s

def asDecodedDBpediaUrl(s):
	return urlDecode(asDBpediaUrl(s))

def urlDecode(s):
	return urllib.parse.unquote(s)

def unquote(s):
	if (re.match("^<[^>]*>", s)):
		s = s.strip("<>")
		#s = urllib.parse.unquote(s)
	return s

def getResourcePath(resource, path):
	ddb_r = asDecodedDBpediaUrl(resource)
	# split prefix and suffix, only works for 3 letter prefixes
	prefix = resource[:3]
	suffix = resource[4:]
	ddb_suffix = ddb_r[4:]
	# just in case suffix is empty or is a dot
	if (not suffix or suffix == '.'):
		suffix = '_'
	# just in case suffix is a dot dot
	if (suffix == '..'):
		suffix = '_.'
	rrr = re.sub(r"^[\./]", '_', ddb_suffix)[:2].upper()
	logging.debug("+++ mkdir " + os.path.join(path, prefix, rrr))
	mkdir(os.path.join(path, prefix, rrr))
	return os.path.join(path, prefix, rrr, suffix)

## File helpers
def openA(path):
	try:
		if (os.path.isfile(path)):
			return open(path, 'a')
		else:
			return open(path, 'w')
	except FileNotFoundError as fnf:
		logging.error("--- File not found: " + path + ":" + fnf)
		return openA(destpath + "filenotfound")

def decompress(path):
	if (path.split('.')[-1] == 'nt'):
		return open(path, 'r')
	if (path.split('.')[-1] == 'bz2'):
		return bz2.BZ2File(path, 'r', 1024)
	else:
		logging.warning("--- Not N-Triples or BZ2 compressed?")
		return None

def mkdir(path):
    try:
        os.makedirs(path)
    except OSError as exc: # Python >2.5
        if exc.errno == errno.EEXIST and os.path.isdir(path):
            pass
        else: raise

def rmdir(path):
	if (os.path.isdir(path)):
	    shutil.rmtree(path)


if __name__ == "__main__":
	main(sys.argv[1:])

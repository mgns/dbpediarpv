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
import urllib.parse
import re
import shutil
import codecs
import concurrent.futures
import sys
#reload(sys)
#sys.setdefaultencoding('utf8')
import getopt
import difflib
from collections import defaultdict, Counter
#print(sys.path)

import datetime
import logging
logger = logging.getLogger('dbpediarpv.' + __name__)
#logging.basicConfig(filename='process.log', format='%(asctime)s %(levelname)s:%(message)s', level=logging.INFO)
logging.basicConfig(format='%(asctime)s %(levelname)s:%(message)s', level=logging.INFO)

config_file = "datasets_core.json"
destpath = "/home/magnus/Datasets/dbpediarpv/"
srcpath = "/home/magnus/Datasets/downloads.dbpedia.org/"
destpath = "/Users/magnus/Datasets/dbpediarpv/"
srcpath = "/Users/magnus/Data/dbpedia/"
clearDest = False

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
	logging.info("Hello.")
	
#	print("<http://dbpedia.org/resource/%21G%C3%A3%21ne_language>")
#	print(prefixify("<http://dbpedia.org/resource/%21G%C3%A3%21ne_language>"))
#	print(unPrefixify(prefixify("<http://dbpedia.org/resource/%21G%C3%A3%21ne_language>")))
#	print(getResourcePath(asDecodedDBpediaUrl(prefixify("<http://dbpedia.org/resource/Articles_for_creation/2006-10-21>"))[4:], destpath))
	
	#print(isResourceRegex)
	#print(prefixifyRegex)
	#print(unPrefixifyRegex)

	try:
		opts, args = getopt.getopt(argv,"",["process","compare","diff","classify"])
	except getopt.GetoptError:
		print('usage: dbpediarpv.py [--process|--compare|--diff|--classify]')
		sys.exit(2)
	for opt, arg in opts:
		if opt == '--process':
			logging.info("##### PROCESS #####")
			versions = readVersions()
			#processVersions(versions)
			processVersionsParallel(versions)
			sys.exit()
		elif opt == '--compare':
			logging.info("##### COMPARE #####")
			versions = readVersions()
			compareVersions(versions)
			sys.exit()
		elif opt == '--diff':
			logging.info("##### DIFF #####")
			versions = readVersions()
			diffVersions(versions)
			sys.exit()
		elif opt == '--classify':
			logging.info("##### CLASSIFY #####")
			versions = readVersions()
			classifyVersions(versions)
			sys.exit()
		else:
			print('usage: dbpediarpv.py [--process|--compare|--diff|--classify]')
			sys.exit(2)
		
def readVersions():
	versions = []
	with open(config_file) as jfile:
		versions = json.load(jfile)
		#versions = json.loads('{ "order": ["1.0","2.0"],\n "version": [ { "id": "1.0", "dir": "1.0/", "files": ["test.nt"] },\n { "id": "2.0", "dir": "2.0/", "files": [ "test.nt" ] } ] }')
	return versions

def processVersions(versions):
	for v in versions["version"]:
		processVersion(v)

def processVersionsParallel(versions):
	with concurrent.futures.ProcessPoolExecutor() as executor:
	#with concurrent.futures.ThreadPoolExecutor(max_workers=4) as executor:
		logging.info("#### Start " + str(len(versions["version"])) + " processes in parallel ####")
		
		futures = executor.map(processVersion, versions["version"])
		for result in futures:
			logging.info("#### " + result + " ####")

def processVersion(version):
	logging.info("### DBpedia " + version["id"] + ": " + str(len(version["files"])) + " files ###")
	vdestpath = os.path.join(destpath, version["id"])
	if (clearDest):
		logging.info("Clear " + vdestpath)
		rmdir(vdestpath)
	mkdir(vdestpath)
	rfile = openA(os.path.join(vdestpath, 'nodbp'))
	for file in version["files"]:
		#try:
		processFile(os.path.join(srcpath, version["dir"], file), vdestpath, rfile)
		#except BaseException as e:
		#	logging.error("--- Error while processing " + os.path.join(srcpath, version["dir"], file) + " : " + str(e))
		#	exit
	rfile.close()
	logging.info("### DBpedia " + version["id"] + " done : " + str(len(version["files"])) + " files ###")
	return "Version " + version["id"] + " done"

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
	maxlines = -10000
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
				
				mkdir(os.path.dirname(f))
				sfile = openA(f)
				sfile.write("\n".join(content) + "\n")
				sfile.close()
			filecontents.clear()
		i = i + 1

	logging.info("## Write contents of file " + fsrcpath + " having " + str(len(filecontents.keys())) + " entries ##")
	for f, content in filecontents.items():
		logging.debug("+++ Write " + f + "\n + " + "\n + ".join(content))
		
		mkdir(os.path.dirname(f))
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

	# Filter subjects 'M*'
	if (not s.startswith('<http://dbpedia.org/')):
		return None, None

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

#@deprecated
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

#@deprecated
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
		
		mkdir(os.path.join(destpath, v2))

		with codecs.open(os.path.join(destpath, v2, 'added'), 'w', encoding='utf-8', errors='ignore') as f:
			f.write("\n".join(cD['added']))
		with codecs.open(os.path.join(destpath, v2, 'remained'), 'w', encoding='utf-8', errors='ignore') as f:
			f.write("\n".join(cD['remained']))
		with codecs.open(os.path.join(destpath, v2, 'removed'), 'w', encoding='utf-8', errors='ignore') as f:
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

def diffVersions(versions):
	order = versions['order']
	logging.debug("Order:" + str(order))
	for idx, val in enumerate(order[:-1]):
		v1 = val
		v2 = order[idx+1]
		logging.info("Diff " + v1 + " -> " + v2)
		v1dir = os.path.join(destpath, v1)
		v2dir = os.path.join(destpath, v2)
		diffdestpath = os.path.join(destpath, "diff", v2)
		with codecs.open(os.path.join(destpath, v2, 'remained'), 'r', encoding='utf-8', errors='ignore') as v1:
		#with open(os.path.join(destpath, v2, 'remained'), 'r') as v1:
			for line in v1:
				line = line.strip()
				v1ResourcePath = getResourcePath("dbr:" + line, v1dir)
				v2ResourcePath = getResourcePath("dbr:" + line, v2dir)
				logging.debug("+ Diff " + v1ResourcePath + " - " + v2ResourcePath)

				diffFile(v1ResourcePath, v2ResourcePath, getResourcePath("dbr:" + line, diffdestpath))

def diffFile(v1ResourcePath, v2ResourcePath, diffdestpath):
	#diffdestparent = os.path.pardir(diffdestpath)
	mkdir(os.path.dirname(diffdestpath))

	with codecs.open(v1ResourcePath, 'r', encoding='utf-8', errors='ignore') as v1:
	#with open(v1ResourcePath, 'r') as v1:
		with codecs.open(v2ResourcePath, 'r', encoding='utf-8', errors='ignore') as v2:
		#with open(v2ResourcePath, 'r') as v2:
			v1Lines = set()
			v2Lines = set()
			added = set()
			deleted = set()
			
			for line in v1.readlines():
				v1Lines.add(line.strip())
			for line in v2.readlines():
				v2Lines.add(line.strip())

			for line in v1Lines:
				if line not in v2Lines:
					deleted.add(line)
			for line in v2Lines:
				if line not in v1Lines:
					added.add(line)

			logging.debug("Added: " + "\n".join(added))
			logging.debug("Removed: " + "\n".join(deleted))
			
			with codecs.open(diffdestpath + "_add", 'w', encoding='utf-8', errors='ignore') as f:
				f.write("\n".join(added))
			with codecs.open(diffdestpath + "_del", 'w', encoding='utf-8', errors='ignore') as f:
				f.write("\n".join(deleted))

def classifyVersions(versions):
	order = versions['order']
	logging.debug("Order:" + str(order))
	for idx, val in enumerate(order[:-1]):
		#Base (before)
		Bp = Counter()
		Bo = Counter()
		#State (after)
		Sp = Counter()
		So = Counter()
		#Exchanged
		Xp = Counter()
		Xo = Counter()
		#Added
		Ap = Counter()
		Ao = Counter()
		#Deleted
		Dp = Counter()
		Do = Counter()

		v1 = val
		v2 = order[idx+1]
		logging.info("Classify " + v1 + " -> " + v2)
		v1dir = os.path.join(destpath, v1)
		v2dir = os.path.join(destpath, v2)
		diffdestpath = os.path.join(destpath, "diff", v2)
		with codecs.open(os.path.join(destpath, v2, 'remained'), 'r', encoding='utf-8', errors='ignore') as f:
		#with open(os.path.join(destpath, v2, 'remained'), 'r') as f:
			for line in f:
				line = line.strip()
				v1ResourcePath = getResourcePath("dbr:" + line, v1dir)
				v2ResourcePath = getResourcePath("dbr:" + line, v2dir)
				diffAddPath = getResourcePath("dbr:" + line, diffdestpath) + "_add"
				diffDelPath = getResourcePath("dbr:" + line, diffdestpath) + "_del"
				logging.debug("+ Classify " + v1ResourcePath + " - " + v2ResourcePath)

				BpR, BoR, SpR, SoR, XpR, XoR, ApR, AoR, DpR, DoR = classifyFile(v1ResourcePath, v2ResourcePath, diffAddPath, diffDelPath)
				Bp += BpR
				Bo += BoR
				Sp += SpR
				So += SoR
				Xp += XpR
				Xo += XoR
				Ap += ApR
				Ao += AoR
				Dp += DpR
				Do += DoR
				
		logging.info("************************************")
		logging.info("***** " + v1 + " -> " + v2 + " *****")
		
		for p in Bp:
			logging.info("** Property " + p + " (" + str(Bp[p]) + ")")
			if Dp[p]:
				logging.info("* deleted                         " + '%5d'%Dp[p] + " = " + str(float(Dp[p])/Bp[p]))
			if Sp[p]:
				logging.info("* stable                          " + '%5d'%Sp[p] + " = " + str(float(Sp[p])/Bp[p]))
			for pX in [ k for k,v in Xp.items() if k.startswith(p + " -> ")]:
				logging.info("* -> " + '%-28s'%pX.split(' ', 2)[2] + " " + '%5d'%Xp[pX] + " = " + str(float(Xp[pX])/Bp[p]))
		

#		print "Xp: "
#		for p in Xp:
#			print p + " " + str(Xp[p]) + "/" + str(Bp[p.split(' ', 1)[0]]) + " = " + str(float(Xp[p])/Bp[p.split(' ', 1)[0]])
		#print "Xo: "
		#print Xo
		#print "Ap: "
		#print Ap
		#print "Ao: "
		#print Ao
		#print "Dp: "
		#print Dp
		#print "Do: "
		#print Do

		logging.info("***** " + v1 + " -> " + v2 + " *****")
		logging.info("************************************")

		v2dir = os.path.join(destpath, v2)

		writeClassifyResults(Bp, Bo, Sp, So, Xp, Xo, Ap, Ao, Dp, Do, v2dir)


def writeClassifyResults(Bp, Bo, Sp, So, Xp, Xo, Ap, Ao, Dp, Do, destpath):

	logging.info("Write results to " + destpath)

	with codecs.open(os.path.join(destpath, "propertyDelete"), 'w', encoding='utf-8', errors='ignore') as fpd:
		for p in Bp:
			if Dp[p]:
				fpd.write(p + "\t" + str(Bp[p]) + "\t" + str(Dp[p]) + "\t" + str(float(Dp[p])/Bp[p]) + "\n")

	with codecs.open(os.path.join(destpath, "propertyStable"), 'w', encoding='utf-8', errors='ignore') as f:
		for p in Bp:
			if Sp[p]:
				f.write(p + "\t" + str(Bp[p]) + "\t" + str(Sp[p]) + "\t" + str(float(Sp[p])/Bp[p]) + "\n")

	with codecs.open(os.path.join(destpath, "propertyExchange"), 'w', encoding='utf-8', errors='ignore') as f:
		for p in Bp:
			for pX in [ k for k,v in Xp.items() if k.startswith(p + " -> ")]:
				f.write(p + "\t" + str(Bp[p]) + "\t" + pX.split(' ', 2)[2] + "\t" + str(Xp[pX]) + "\t" + str(float(Xp[pX])/Bp[p]) + "\n")


def classifyFile(v1ResourcePath, v2ResourcePath, diffAddPath, diffDelPath):
	#Base (before)
	Bp = Counter()
	Bo = Counter()
	#State (after)
	Sp = Counter()
	So = Counter()
	#Exchanged
	Xp = Counter()
	Xo = Counter()
	#Added
	Ap = Counter()
	Ao = Counter()
	#Deleted
	Dp = Counter()
	Do = Counter()

	with codecs.open(v1ResourcePath, 'r', encoding='utf-8', errors='ignore') as v1:
	#with open(v1ResourcePath, 'r') as v1:
		with codecs.open(v2ResourcePath, 'r', encoding='utf-8', errors='ignore') as v2:
		#with open(v2ResourcePath, 'r') as v2:
			with codecs.open(diffAddPath, 'r', encoding='utf-8', errors='ignore') as v2add:
			#with open(diffAddPath, 'r') as v2add:
				with codecs.open(diffDelPath, 'r', encoding='utf-8', errors='ignore') as v2del:
				#with open(diffDelPath, 'r') as v2del:
					v1Lines = set()
					v1p = set()
					v1o = set()
					v2Lines = set()
					v2p = set()
					v2o = set()
					added = set()
					addedp = set()
					addedo = set()
					deleted = set()
					deletedp = set()
					deletedo = set()
					
					for line in v1.readlines():
						v1Lines.add(line.strip())
						p = line.strip().split(' ', 1)[0]
						o = line.strip().split(' ', 1)[1]
						Bp[p] += 1
						Bo[o] += 1
						v1p.add(p)
						v1o.add(o)
					for line in v2.readlines():
						v2Lines.add(line.strip())
						p = line.strip().split(' ', 1)[0]
						o = line.strip().split(' ', 1)[1]
						Sp[p] += 1
						So[o] += 1
						v2p.add(p)
						v2o.add(o)
					for line in v2add.readlines():
						added.add(line.strip())
						addedp.add(line.strip().split(' ', 1)[0])
						addedo.add(line.strip().split(' ', 1)[1])
					for line in v2del.readlines():
						deleted.add(line.strip())
						deletedp.add(line.strip().split(' ', 1)[0])
						deletedo.add(line.strip().split(' ', 1)[1])

					for po in added:
						p = po.split(' ', 1)[0]
						o = po.split(' ', 1)[1]
						if (p in v1p): # p was there
							#if (po in v1Lines): # po was there ??
							#	print "ERROR " + po + " added, but was there"
							v1poO = set()
							v2poO = set()
							for v1po in v1Lines:
								if (v1po.split(' ', 1)[0] == p):
									v1poO.add(v1po.split(' ', 1)[1]) ## old objects for p
							for v2po in v2Lines:
								if (v2po.split(' ', 1)[0] == p):
									v2poO.add(v2po.split(' ', 1)[1]) ## new objects for p
							
							if intersect(v1poO, v2poO): # po was added to po'
								#print "Added object for " + p + " (" + o + ") was (" + ', '.join(v1poO) + ") A"
								Ao[o] += 1
							else: # o' -> o
								#print "Exchanged object for " + p + " (" + ', '.join(v1poO) + ") -> (" + o + ") A"
								for oo in v1poO:
									Xo[oo + " -> " + o] += 1
									if (p + " " + oo) in deleted:
										deleted.remove(p + " " + oo)
						else: # p was not there
							v1poP = set()
							v2poP = set()
							for v1po in v1Lines:
								if (v1po.split(' ', 1)[1] == o):
									v1poP.add(v1po.split(' ', 1)[0]) ## old predicates for o
							for v2po in v2Lines:
								if (v2po.split(' ', 1)[1] == o):
									v2poP.add(v2po.split(' ', 1)[0]) ## new predicates for o
							
							if v1poP:
								if intersect(v1poP, v2poP): # po was added to p'o
									#print "Added predicate for " + o + " (" + p + ") was (" + ', '.join(v1poP) + ") A"
									Ap[p] += 1
								else: # p' -> p
									logging.debug("Exchanged predicate for " + o + " (" + ', '.join(v1poP) + ") -> (" + p + ") A " + v1ResourcePath)
									for pp in v1poP:
										Xp[pp + " -> " + p] += 1
										if (pp + " " + o) in deleted:
											deleted.remove(pp + " " + o)

							else: # po is absolutely new
								#print "New triple (" + po + ") A"
								Ap[p] += 1
								Ao[o] += 1
					for po in deleted:
						p = po.split(' ', 1)[0]
						o = po.split(' ', 1)[1]
						if (p in v2p): # p is still there
							v1poO = set()
							v2poO = set()
							for v1po in v1Lines:
								if (v1po.split(' ', 1)[0] == p):
									v1poO.add(v1po.split(' ', 1)[1]) ## old objects for p
							for v2po in v2Lines:
								if (v2po.split(' ', 1)[0] == p):
									v2poO.add(v2po.split(' ', 1)[1]) ## new objects for p
							
							if intersect(v1poO, v2poO): # po was deleted, po' is still there
								#print "Removed object for " + p + " (" + o + ") is (" + ', '.join(v2poO) + ") D"
								Do[o] += 1
							else: # o' -> o
								#print "Exchanged object for " + p + " (" + o + ") -> (" + ', '.join(v2poO) + ") D"
								for oo in v2poO:
									Xo[o + " -> " + oo] += 1
						else: # p is not there anymore
							v1poP = set()
							v2poP = set()
							for v1po in v1Lines:
								if (v1po.split(' ', 1)[1] == o):
									v1poP.add(v1po.split(' ', 1)[0]) ## old predicates for o
							for v2po in v2Lines:
								if (v2po.split(' ', 1)[1] == o):
									v2poP.add(v2po.split(' ', 1)[0]) ## new predicates for o
							
							if v2poP:
								if intersect(v1poP, v2poP): # po was added to p'o
									#print "Removed predicate for " + o + " (" + p + ") is (" + ', '.join(v2poP) + ") D"
									Dp[p] += 1
								else: # p -> p'
									logging.debug("Exchanged predicate for " + o + " (" + p + ") -> (" + ', '.join(v2poP) + ") D " + v1ResourcePath)
									for pp in v2poP:
										Xp[p + " -> " + pp] += 1
							else: # po is absolutely deleted
								#print "Del triple (" + po + ") D"
								Dp[p] += 1
								Do[o] += 1

	return Bp, Bo, Sp, So, Xp, Xo, Ap, Ao, Dp, Do

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
		#s = urlparse.unquote(s)
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

	#logging.info("+++ DEBUG : " + resource + " -> " + rrr)

	#mkdir(os.path.join(path, prefix, rrr))
	return os.path.join(path, prefix, rrr, suffix)

## Set helpers
def intersect(a, b):
	return list(set(a) & set(b))

## File helpers
def openA(path):
	try:
		if (os.path.isfile(path)):
			return codecs.open(path, 'a', encoding='utf-8', errors='ignore')
		else:
			return codecs.open(path, 'w', encoding='utf-8', errors='ignore')
	except IOError as fnf:
		logging.error("--- IO error: " + path + ":" + fnf)
		return openA(destpath + "filenotfound")

def decompress(path):
	if (path.split('.')[-1] == 'nt'):
		return open(path, 'r')
	if (path.split('.')[-1] == 'bz2'):
		#return bz2.BZ2File(path, 'r', 1024)
		return os.popen('bzip2 -cd ' + path)
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

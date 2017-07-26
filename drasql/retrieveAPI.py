#!/usr/bin/python


# define a function extract information from uniiprot API

def uniprotAPI(query):
	'''returns <full name of the query>, <species>, <taxonomy>
'''


	import urllib
	import xml.etree.ElementTree as ET

	serviceurl = 'http://www.uniprot.org/uniprot/'

	full_name = ""
	species_name = ""
	uniprot_tax = ""

	#name space
	ns = '{http://uniprot.org/uniprot}'
	url = serviceurl + str(query) + '.xml'

	#read xml page
	page = urllib.urlopen(url).read()

	# load into tree
	root = ET.fromstring(page)

	# iterate in the tree: two roots: entry, copyright
	for item in root.find(ns + 'entry'):
		# nested in the protein tag
		if item.tag == (ns + 'protein'):
			for i in item:
				if i.tag == (ns + 'recommendedName'):
					# full name is under recommended name
					fn = i.find(ns + 'fullName')
					full_name = fn.text





		# get organism name
		if item.tag == (ns + 'organism'):

			for i in item:



				# get the species name the query comes from
				# < name type = "scientific">...</name>
				if i.tag == (ns + 'name'):
					species_name = i.text.split('(')[0].strip()

				# get taxonomy, e.g bacteria
				if i.tag == (ns + 'lineage'):
					tax = i.find(ns + 'taxon')
					uniprot_tax = tax.text





	return full_name, species_name, uniprot_tax


### this function use to retrieve kegg api ################


def kegg(KO, entry):

	"""input KO and entry identifier to get KEGG ORTHOLOGY, kegg full name, kegg organism
	KO format - KO:K02897
	entry format - dra:DR_1503
"""


	from bs4 import BeautifulSoup as BS
	import urllib

	KO = str(KO)

	geneID = str(entry)

	KoOrth = ""
	KoRef_def = ""
	geneName = ""
	species = ""


	# get the url

	baseurl = 'http://rest.kegg.jp/list/'
	orgbaseurl = 'http://www.kegg.jp/kegg-bin/show_organism?'
	kobase = 'ko:'
	add_url = 'http://www.kegg.jp/entry/'

	# 4 urls
	ko_url = baseurl + kobase + KO # url 1: for kegg orthology
	gene_url = baseurl + geneID # url 2: for kegg genome entry

	para = {'org':geneID.split(':')[0]}
	org_suffix = urllib.urlencode(para)
	orgurl = orgbaseurl + org_suffix # url 3: for kegg organism database
	addendumurl = add_url + geneID



	if len(KO) != 0 and len(entry) != 0:


		# KEGG ORTHOLOGY
		koPage = urllib.urlopen(ko_url).read()

		split_into = koPage.split(';')


		#if len(split_into) != 0:

		koOrth = "KEGG-orth:" + split_into[1].strip().split('[')[0].strip()
		geneName = split_into[0].split('\t')[1].strip()


		# KEGG GENOME database to get the full name
		genomePage = urllib.urlopen(gene_url).read()

		koRef_def = "KEGG-genome:" + genomePage.split('|')[1].strip()

		# ORGANISM database
		html_org = urllib.urlopen(orgurl).read()
		soup = BS(html_org, 'html.parser')
		if soup.title.string == 'Usage':
			add_page = urllib.urlopen(addendumurl).read()
			add_soup = BS(add_page, 'html.parser')
			divs = add_soup.find_all('div')

			for item in divs:
				stuff = item.contents

				for each in stuff:

					if 'Addendum' in each:
						species = each.split('(')[1].split(')')[0].strip()

						species = 'KEGG-Addendum-org:' + species.encode('ascii')


		else:
			species = soup.title.string.split(':')[1].strip()
			species = "KEGG-org:" + species.encode('ascii')



	elif KO == '' and len(entry) != 0:


		koOrth = 'No_entry'
		geneName = 'No_entry'

                # get the url


		# KEGG GENOME database to get the full name
                genomePage = urllib.urlopen(gene_url).read()

                koRef_def = "KEGG-genome:" + genomePage.split('|')[1].strip()

                # ORGANISM database
                html_org = urllib.urlopen(orgurl).read()
                soup = BS(html_org, 'html.parser')

		# for the id is in addendum
		if soup.title.string == 'Usage':
			add_page = urllib.urlopen(addendumurl).read()
			add_soup = BS(add_page, 'html.parser')
			divs = add_soup.find_all('div')
			for item in divs:
				stuff = item.contents
				for each in stuff:
					if 'Addendum' in each:
						species = each.split('(')[1].split(')')[0].strip()
						species = "KEGG-Addendum-org:" + species.encode('ascii')


		else:
                	species = soup.title.string.split(':')[1].strip()

	                species = "KEGG-org:" + species.encode('ascii')



	return koOrth, koRef_def, geneName, species



############# run a program to test


import sqlite3
import csv
import time


conn = sqlite3.connect("Drad-Annotation-simple.sqlite")
cur = conn.cursor()

cur.execute('''create table if not exists Drad_Anno_simple (
		gene_id text,
		blx_full text,
		blx_species text,
		blx_tax text,
		blp_full text,
		blp_species text,
		blp_tax text,
		kegg_ID text,
		kegg_geneName text,
		kegg_KO text,
		kegg_orth text,
		kegg_Ref text,
		kegg_species text
		)
''')



filename = 'simple_annotation'

with open(filename, 'rb') as f:

	file = csv.reader(f, delimiter = '\t')

	file.next() # skip the header



	# uniprot

	gene_ids = list()

	# blastx
	bx_full_def = list()
	bx_species_names = list()
	bx_tax = list()


	#blastp
	bp_full_def = list()
	bp_species_names = list()
	bp_tax = list()

	# KEGG
	kid = list()
	KOid = list()
	keggOrth = list()
	keggRef = list()
	kegg_geneName = list()
	keggSpecies = list()



	n = 0

	for row in file:

		n += 1

		if n <= 4900: continue

		else:
			gene_ids.append(row[0])


		# to get full definition of the query
			if row[2] == '0':
				nohit = 'No_hits'
				bx_full_def.append(nohit)
				bx_species_names.append(nohit)
				bx_tax.append(nohit)



			else:
				query = row[2].split('^')[0]
				fs = uniprotAPI(query)
				bx_full_def.append(str("Blx:" + fs[0]))
				bx_species_names.append(str("Blx:" + fs[1]))
				bx_tax.append(str("Blx:" + fs[2]))



			if row[6] == '0':

				nohit = 'No_hits'
				bp_full_def.append(nohit)
				bp_species_names.append(nohit)
				bp_tax.append(nohit)




			else:
				query = row[6].split('^')[0]
				fx = uniprotAPI(query)
				bp_full_def.append(str("Blp:" + fx[0]))
				bp_species_names.append(str("Blp:" + fx[1]))
				bp_tax.append(str("Blp:" + fx[2]))




			if row[11] == '0':

			# KEGG
       			# kid = list()
        		# keggOrth = list()
        		# keggRef = list()
       			# keggFul = list()
        		# keggSpecies = list()

				nohit = 'No_hits'

				kid.append(nohit)
				KOid.append(nohit)
				keggOrth.append(nohit)
				keggRef.append(nohit)
				kegg_geneName.append(nohit)
				keggSpecies.append(nohit)





			else:
				split_row11 = row[11].split('`')

				if len(split_row11) > 1:

					genomeID = split_row11[0][5:].strip() # <org>:<identifier>

					koID = split_row11[-1].split(':')[1].strip() # KO:####

					if koID.startswith('KO'):

				# koOrth, koRef_def, geneName,  species

						api = kegg(koID, genomeID)
						kid.append(genomeID)
						KOid.append(koID)
						keggOrth.append(api[0])
						keggRef.append(api[1])
						kegg_geneName.append(api[2])
						keggSpecies.append(api[3])




					else:
						koID = ''
						api = kegg(koID, genomeID)
                                       		kid.append(genomeID)
                                        	KOid.append(koID)
                                        	keggOrth.append(api[0])
                                        	keggRef.append(api[1])
                                        	kegg_geneName.append(api[2])
                                        	keggSpecies.append(api[3])







				else:
					genomeID = split_row11[0][5:].strip()
					koID = ''

				 # koOrth, koRef_def, geneName,  species

                               		api = kegg(koID, genomeID)
					kid.append(genomeID)
					KOid.append(koID)
                                	keggOrth.append(api[0])
                                	keggRef.append(api[1])
                                	kegg_geneName.append(api[2])
                                	keggSpecies.append(api[3])






		print "row number: {}".format(n)

		time.sleep(1)



###########################################
	# uniprot

	#gene_ids = list()

	# blastx
	#bx_full_def = list()
	#bx_species_names = list()
	#bx_tax = list()


	#blastp
	#bp_full_def = list()
	#bp_species_names = list()
	#bp_tax = list()

	# KEGG
	#kid = list()
	#KOid = list()
	#keggOrth = list()
	#keggRef = list()
	#kegg_geneName = list()
	#keggSpecies = list()


for i in range(len(gene_ids)):

	gid = gene_ids[i]
	bx_full = bx_full_def[i]
	bx_species = bx_species_names[i]
	bx_taxon = bx_tax[i]
	bp_full = bp_full_def[i]
	bp_species = bp_species_names[i]
	bp_taxon = bp_tax[i]
	kegID = kid[i]
	koid = KOid[i]
	kgOrth = keggOrth[i]
	kgRef = keggRef[i]
	kgGeneN = kegg_geneName[i]
	kgSp = keggSpecies[i]



	cur.execute('''INSERT INTO Drad_Anno_simple (gene_id, kegg_geneName,  kegg_ID, kegg_KO, 
			kegg_Ref, kegg_orth, kegg_species, blx_full, blx_species, blx_tax, blp_full, blp_species, blp_tax) VALUES (?,?,?,?,?,?,?,?,?,?,?,?, ?)''',
			(gid, kgGeneN, kegID, koid, kgRef, kgOrth, kgSp, bx_full, bx_species, bx_taxon, bp_full, bp_species, bp_taxon, )) 





conn.commit()
cur.close()



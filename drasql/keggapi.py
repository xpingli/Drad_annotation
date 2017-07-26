#!/usr/bin/python

def kegg(KO, entry):

	"""input KO and entry identifier to get KEGG ORTHOLOGY, kegg full name, kegg organism
	KO format - KO:K02897
	entry format - dra:DR_1503
"""


	from bs4 import BeautifulSoup as BS
	import urllib


	geneID = entry

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



	elif len(KO) == 0 and len(entry) > 0:


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



filename = 'simple_annotation'

import csv
import sqlite3
import time

conn = sqlite3.connect('keggSQL.sqlite')
cur = conn.cursor()


cur.execute('''create table if not exists Dra_kegg_sqlite (
		Gene_id text,
		KeggOrth_identifier text,
		KeggOrth text,
		KeggSpecies text,
		Kegg_Gene text,
		Kegg_Gene_annotation text
)''')


with open(filename, 'rb') as csvfile:

	anno = csv.reader(csvfile, delimiter = '\t')

	anno.next()

	n = 0
	gene_id = list()
	KO_id = list()
	keggOrth = list()
	keggSpec = list()
	keggRef = list()
	keggGeneName = list()

	for row in anno:

		n += 1


		if n <= 4000: continue

		gene_id.append(row[0])

		# get gene id entry:
		if row[11] == '0':
			KO_id.append('No hit')
			keggOrth.append('No hit')
			keggSpec.append('No hit')
			keggRef.append('No hit')
			keggGeneName.append('No hit')
		else:

			if len(row[11].split('`')) > 1:

				if not row[11].split('`')[-1].startswith('KEGG'):
					geneid = row[11].split('`')[0][5:]

					koid = row[11].split('`')[-1].split(':')[-1]

					api = kegg(koid, geneid)

					KO_id.append(koid)
					keggOrth.append(api[0])
					keggSpec.append(api[3])
					keggRef.append(api[1])
					keggGeneName.append(api[2])
				else:
					geneid = row[11].split('`')[0][5:]

                                        koid = ''

                                        api = kegg(koid, geneid)

                                        KO_id.append(koid)
                                        keggOrth.append(api[0])
                                        keggSpec.append(api[3])
                                        keggRef.append(api[1])
                                        keggGeneName.append(api[2])

			elif len(row[11].split('`')) == 1:
				geneid = row[11][5:]
				koid = ('')

				api = kegg(koid, geneid)

				KO_id.append(koid)
				keggOrth.append(api[0])
                                keggSpec.append(api[3])
                                keggRef.append(api[1])
                                keggGeneName.append(api[2])





		print "row number: {}".format(n)
		time.sleep(1)



for i in range(len(gene_id)):

	gid = gene_id[i]

	kid = KO_id[i]
        kOrth = keggOrth[i]
        kSpec = keggSpec[i]
        kRef = keggRef[i]
        kGN = keggGeneName[i]





	cur.execute('''insert into Dra_kegg_sqlite (
			Gene_id,
			KeggOrth_identifier,
			KeggOrth,
			KeggSpecies,
			Kegg_Gene,
			Kegg_Gene_annotation ) values (
			?, ?, ?, ?, ?,?)''', ( gid, kid, kOrth, kSpec, kGN, kRef,))



conn.commit()
cur.close()


# python2
import lucene
from java.nio.file import Paths
from org.apache.lucene.analysis.miscellaneous import LimitTokenCountAnalyzer
from org.apache.lucene.analysis.standard import StandardAnalyzer
from org.apache.lucene.index import IndexWriter, IndexWriterConfig
from org.apache.lucene.document import Document, Field, StringField, TextField
from org.apache.lucene.store import SimpleFSDirectory
from org.apache.lucene.queryparser.classic import QueryParser
from org.apache.lucene.search import IndexSearcher
from org.apache.lucene.index import DirectoryReader

import json
import sys
import os


def data_cleaning(table_in, table_out):
    eids = set()
    with open(table_in, 'r') as fin:
        with open(table_out, 'w') as fout:
            for line in fin:
                tokens = line.strip('\n').split('\t')
                origin, etype, eid, name = tokens[0], tokens[1], tokens[2], tokens[3]
                if eid in eids:
                    continue
                if origin == 'GEO':
                    country_code = tokens[12]
                    wiki_link = tokens[46]
                    if country_code != 'RU' and country_code != 'UA' and wiki_link == '':
                        continue
                eids.add(eid)
                fout.write(line)


def load_id2name(kb_path, alias_path):
    id2type = {}
    id2info = {}
    with open(kb_path, 'r') as f:
        f.readline()
        for line in f:
            tokens = line[:-1].split('\t')
            eid, name, type = tokens[2], tokens[3], tokens[1]
            src = tokens[0]
            if src == 'GEO':
                info = tokens[12]
            elif src == 'WLL':
                info = '\t'.join([tokens[26], tokens[27], tokens[28]])
            elif src == 'APB':
                print(tokens)
                info = tokens[35]
                print(info)
            else:
                info = ''
            id2type[eid] = type
            id2info[eid] = info
            yield eid, name, type, info
    with open(alias_path, 'r') as f:
        f.readline()
        for line in f:
            eid, name = line.strip().split('\t')
            if eid in id2type:
                yield eid, name, id2type[eid], id2info[eid]


class Indexer:
    def __init__(self, indexDir):
        self.directory = SimpleFSDirectory(Paths.get(indexDir))
        self.analyzer = StandardAnalyzer()
        # analyzer = LimitTokenCountAnalyzer(analyzer, 10000)
        self.config = IndexWriterConfig(self.analyzer)
        self.writer = IndexWriter(self.directory, self.config)

    def index(self, eid, name, type, info):
        doc = Document()
        doc.add(TextField('id', eid, Field.Store.YES))
        doc.add(TextField('name', name, Field.Store.YES))
        doc.add(TextField('type', type, Field.Store.YES))
        doc.add(TextField('info', info, Field.Store.YES))
        self.writer.addDocument(doc)
        # print eid, name

    def close(self):
        self.writer.commit()
        self.writer.close()


class Searcher:
    def __init__(self, indexDir):
        self.directory = SimpleFSDirectory(Paths.get(indexDir))
        self.searcher = IndexSearcher(DirectoryReader.open(self.directory))
        self.nameQueryParser = QueryParser('name', StandardAnalyzer())
        self.nameQueryParser.setDefaultOperator(QueryParser.Operator.AND)
        self.idQueryParser = QueryParser('id', StandardAnalyzer())
        self.idQueryParser.setDefaultOperator(QueryParser.Operator.AND)

    def find_by_name(self, name):
        query = self.nameQueryParser.parse(name)
        docs = self.searcher.search(query, 100).scoreDocs
        tables = []
        for scoreDoc in docs:
            doc = self.searcher.doc(scoreDoc.doc)
            table = dict((field.name(), field.stringValue()) for field in doc.getFields())
            tables.append(table)
        
        return tables

    def find_by_id(self, id):
        query = self.idQueryParser.parse(id)
        docs = self.searcher.search(query, 100).scoreDocs
        tables = []
        for scoreDoc in docs:
            doc = self.searcher.doc(scoreDoc.doc)
            table = dict((field.name(), field.stringValue()) for field in doc.getFields())
            tables.append(table)
        
        return tables

class EntityLinker(object):
    def __init__(self):
        self.searcher = Searcher('lucene_index/')
        
    def query(self, ne):
        ent_name, ent_type = ne['mention'].lower(), ne['type'][7:10]
        print(ent_name, ent_type)
        try:
            candidates = self.searcher.find_by_name(ent_name)
        except:
            return 'none'

        if ent_type == 'GPE' or ent_type == 'LOC' or ent_type == 'FAC':
            candidates = filter(lambda x: x['type'] in ['GPE', 'LOC'], candidates)
        elif ent_type == 'ORG':
            candidates = filter(lambda x: x['type'] == 'ORG', candidates)
        elif ent_type == 'PER':
            candidates = filter(lambda x: x['type'] == 'PER', candidates)
        else:
            return 'none'

        # remove duplication
        candidate_ids = set()
        filtered_candidates = []
        for candidate in candidates:
            if candidate['id'] in candidate_ids:
                continue
            candidate_ids.add(candidate['id'])
            filtered_candidates.append(candidate)
        candidates = filtered_candidates
        if len(candidates) == 1:
            return candidates[0]

        # find exact match
        filtered = filter(lambda x: x['name'].lower() == ent_name, candidates)
        if len(filtered) == 1:
            return filtered[0]['id']
        elif len(filtered) == 0:
            return 'none'
        candidates = filtered

        # filter by type
        filtered = filter(lambda x: x['type'] == ent_type, candidates)
        if len(filtered) == 1:
            return filtered[0]['id']
        elif len(filtered) == 0:
            return 'none'
        candidates = filtered

        # filter by country
        filtered = filter(lambda x: x['type'] != 'GPE' and x['type'] != 'LOC' or x['info'] == 'RU' or x['info'] == 'UA', candidates)
        if len(filtered) == 1:
            return filtered[0]['id']
        elif len(filtered) == 0:
            return 'none'
        candidates = filtered
        
        print(candidates)
        return candidates[0]['id']

if __name__ == '__main__':
    # data_cleaning('LDC2018E80_LORELEI_Background_KB/data/entities.tab', 'LDC2018E80_LORELEI_Background_KB/data/cleaned.tab')
    # lucene.initVM(vmargs=['-Djava.awt.headless=true'])
    # os.system('rm -rf lucene_index/')
    # indexer = Indexer('lucene_index/')
    # for eid, name, type, info in load_id2name('LDC2018E80_LORELEI_Background_KB/data/cleaned.tab', 'LDC2018E80_LORELEI_Background_KB/data/alternate_names.tab'):
    #     indexer.index(eid, name, type, info)
    # indexer.close()

    lucene.initVM(vmargs=['-Djava.awt.headless=true'])
    linker = EntityLinker()
    input_dir = sys.argv[1]
    for fname in os.listdir(input_dir):
        input_file = os.path.join(input_dir, fname)
        print input_file
        with open(input_file, 'r') as f:
            json_doc = json.load(f)
        for sentence in json_doc:
            for ner in sentence['namedMentions']:
                result = linker.query(ner)
                print(result)
                ner['link_lorelei'] = result
        with open(input_file, 'w') as f:
            json.dump(json_doc, f, indent=1, sort_keys=True)

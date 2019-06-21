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
from collections import defaultdict


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
    id2name = {}
    id2type = {}
    id2info = {}
    with open(kb_path, 'r') as f:
        f.readline()
        for line in f:
            tokens = line[:-1].split('\t')
            eid, name, type = tokens[2], tokens[3], tokens[1]
            src = tokens[0]
            if src == 'GEO':
                info = '{}\t{}'.format(tokens[12], tokens[8])
            elif src == 'WLL':
                info = '\t'.join([tokens[26], tokens[27], tokens[28]])
            elif src == 'APB':
                info = tokens[35]
            else:
                info = ''
            id2name[eid] = name
            id2type[eid] = type
            id2info[eid] = info
            yield eid, name, name, type, info
    with open(alias_path, 'r') as f:
        f.readline()
        for line in f:
            eid, name = line.strip().split('\t')
            if eid in id2type:
                yield eid, name, id2name[eid], id2type[eid], id2info[eid]


class Indexer:
    def __init__(self, indexDir):
        self.directory = SimpleFSDirectory(Paths.get(indexDir))
        self.analyzer = StandardAnalyzer()
        # analyzer = LimitTokenCountAnalyzer(analyzer, 10000)
        self.config = IndexWriterConfig(self.analyzer)
        self.writer = IndexWriter(self.directory, self.config)

    def index(self, eid, name, cname, type, info):
        doc = Document()
        doc.add(TextField('id', eid, Field.Store.YES))
        doc.add(TextField('name', name, Field.Store.YES))
        doc.add(TextField('CannonicalName', cname, Field.Store.YES))
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

def iou(str1, str2):
    tokens1 = set(str1.split())
    tokens2 = set(str2.split())
    return float(len(tokens1 & tokens2)) / len(tokens1 | tokens2)

class EntityLinker(object):
    def __init__(self):
        self.searcher = Searcher('lucene_index/')

    def search_candidates(self, name, dist=0):
        if dist == 0:
            return self.searcher.find_by_name(name)
        else:
            terms = name.split(' ')
            query = ' '.join(['{}~{}'.format(term, dist) for term in terms])
            # print(query)
            return self.searcher.find_by_name(query)
        
    def filter_candidates(self, candidates, ent_name, ent_type):
        # filter by type
        if ent_type == 'GPE' or ent_type == 'LOC' or ent_type == 'FAC':
            candidates = filter(lambda x: x['type'] in ['GPE', 'LOC'], candidates)
        elif ent_type == 'ORG':
            candidates = filter(lambda x: x['type'] == 'ORG', candidates)
        elif ent_type == 'PER':
            candidates = filter(lambda x: x['type'] == 'PER', candidates)
        else:
            return None
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
            return candidates

        # find exact match
        filtered = filter(lambda x: x['name'].lower() == ent_name, candidates)
        if len(filtered) == 1:
            return filtered
        elif len(filtered) == 0:
            pass
        else:
            candidates = filtered

        # filter by type
        filtered = filter(lambda x: x['type'] == ent_type, candidates)
        if len(filtered) == 1:
            return filtered
        elif len(filtered) == 0:
            return None
        else:
            candidates = filtered

        # filter by country
        filtered = filter(lambda x: x['type'] != 'GPE' and x['type'] != 'LOC' 
            or x['info'][:2] == 'RU' or x['info'][:2] == 'UA' or x['info'][3:] == 'country,state,region,...', candidates)
        if len(filtered) == 1:
            return filtered
        elif len(filtered) == 0:
            return None
        else:
            candidates = filtered

        return candidates

    def disamb(self, candidates, ent_name, ent_type, sentence):
        # print 'disamb:', candidates
        edit_score = [1./(abs(len(candidate['name']) - len(ent_name)) + 1) for candidate in candidates]
        context_score = [0 for _ in range(len(candidates))]
        if ent_type == 'PER':
            for c, candidate in enumerate(candidates):
                info = candidate['info']
                context_score[c] = iou(info, sentence) * 5
                if 'Russia' in info or 'Ukraine' in info:
                    context_score[c] += 1
        elif ent_type == 'ORG':
            for c, candidate in enumerate(candidates):
                info = candidate['info']
                context_score[c] = iou(info, sentence) * 5
        
        scores = [0 for _ in range(len(candidates))]
        for i in range(len(candidates)):
            scores[i] = edit_score[i] + context_score[i]
        # print scores
        score_sum = sum(scores)
        for i in range(len(candidates)):
            candidates[i]['confidence'] = scores[i] / score_sum
        candidates.sort(key=lambda x: -x['confidence'])
        return candidates

    def query(self, ne, sentence):
        ent_name, ent_type = ne['mention'].lower(), ne['type'][7:10]
        # print(ent_name, ent_type)

        candidates = self.search_candidates(ent_name, 0)
        # print candidates
        candidates = self.filter_candidates(candidates, ent_name, ent_type)
        # print candidates
        if candidates is None or len(candidates) == 0:
            for dist in range(min(5, len(ent_name)//5)):
                candidates = self.search_candidates(ent_name, dist+1)
                # print candidates
                candidates = self.filter_candidates(candidates, ent_name, ent_type)
                # print candidates
                if candidates is not None and len(candidates) > 0:
                    break
        
        if candidates is None or len(candidates) == 0:
            return 'none'
        if len(candidates) == 1:
            candidates[0]['confidence'] = 1.0
            return candidates
        return self.disamb(candidates, ent_name, ent_type, sentence)

class TemporaryKB(object):
    def __init__(self):
        if os.path.isdir('tmp_index/'):
            with open('tmp_index/count.txt', 'r') as f:
                self.count = int(f.readline().strip())
            # self.indexer = Indexer('tmp_index/')
            self.searcher = Searcher('tmp_index/')
        else:
            os.mkdir('tmp_index/')
            self.count = 0
            with open('tmp_index/count.txt', 'w') as f:
                f.write('{}'.format(self.count))
            # self.indexer = Indexer('tmp_index/')
            self.register('MH17', 'VEH')
            
            self.searcher = Searcher('tmp_index/')
            self.register('T-34', 'VEH')

    def register(self, name, type):
        print 'registering:', name, type
        indexer = Indexer('tmp_index/')
        indexer.index('@{}'.format(self.count), name, name, type, '')
        self.count += 1
        with open('tmp_index/count.txt', 'w') as f:
            f.write('{}'.format(self.count))
        indexer.close()
        # print '$$', self.searcher.find_by_name(name)

    def query(self, ne):
        ent_name, ent_type = ne['mention'].lower(), ne['type'][7:10]
        # print(ent_name, ent_type)
        results = self.searcher.find_by_name(ent_name)
        # print(results)
        results = filter(lambda x: x['type'] == ent_type, results)
        if results is None or len(results) == 0:
            return  'none'
        return results


if __name__ == '__main__':
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument('--index', action='store_true')
    parser.add_argument('--query', action='store_true')
    parser.add_argument('--run', action='store_true')
    parser.add_argument('--run_csr', action='store_true')
    parser.add_argument('--dir', type=str)
    args = parser.parse_args()

    if args.index:
        data_cleaning('LDC2018E80_LORELEI_Background_KB/data/entities.tab', 'LDC2018E80_LORELEI_Background_KB/data/cleaned.tab')
        lucene.initVM(vmargs=['-Djava.awt.headless=true'])
        os.system('rm -rf lucene_index/')
        indexer = Indexer('lucene_index/')
        for eid, name, cname, type, info in load_id2name('LDC2018E80_LORELEI_Background_KB/data/cleaned.tab', 'LDC2018E80_LORELEI_Background_KB/data/alternate_names.tab'):
            indexer.index(eid, name, cname, type, info)
        indexer.close()
    elif args.run:
        lucene.initVM(vmargs=['-Djava.awt.headless=true'])
        linker = EntityLinker()
        tmpkb = TemporaryKB()
        input_dir = args.dir
        for fname in os.listdir(input_dir):
            input_file = os.path.join(input_dir, fname)
            print input_file
            with open(input_file, 'r') as f:
                json_doc = json.load(f)
            null_ents = []
            for sentence in json_doc:
                sent_text = sentence['inputSentence']
                for ner in sentence['namedMentions']:
                    try:
                        result = linker.query(ner, sent_text)
                        # print result
                        ner['link_lorelei'] = result
                        if result == 'none':
                            null_ents.append(ner)
                    except:
                        ner['link_lorelei'] = 'none'
                        # print 'none'
            # print(null_ents)
            for null_ent in null_ents:
                result = tmpkb.query(null_ent)
                null_ent['link_lorelei'] = result
            null_counter = defaultdict(int)
            null_ents = filter(lambda x: x['link_lorelei'] == 'none', null_ents)
            for null_ent in null_ents:
                null_counter[(null_ent['mention'].lower(), null_ent['type'][7:10])] += 1
            for (name, type), count in null_counter.items():
                if count >= 5:
                    tmpkb.register(name, type)
            # tmpkb.register('test', 'test')
            
            with open(input_file, 'w') as f:
                json.dump(json_doc, f, indent=1, sort_keys=True)
    elif args.run_csr:
        lucene.initVM(vmargs=['-Djava.awt.headless=true'])
        linker = EntityLinker()
        tmpkb = TemporaryKB()
        input_dir = args.dir
        for fname in os.listdir(input_dir):
            input_file = os.path.join(input_dir, fname)
            print input_file
            with open(input_file, 'r') as f:
                json_doc = json.load(f)
            null_ents = []
            for frame in json_doc['frames']:
                if frame['@type'] != 'entity_evidence':
                    continue
                text = frame['provenance']['text'].encode('utf-8')
                type = frame['interp']['type']
                fringe = frame['interp']['fringe'] if 'fringe' in frame['interp'] else None
                print text, type, fringe
                ne = {'mention': text, 'type': type}
                result = linker.query(ne, '')
                if not fringe is None:
                    fne = {'mention': text, 'type': type}
                    fresult = linker.query(fne, '')
                if result != 'none':
                    print result
                    if 'xref' not in frame['interp']:
                        frame['interp']['xref'] = []
                    frame['interp']['xref'].append({"@type": "db_reference", 
                        "component": "opera.entities.edl.refkb.xianyang",
                        "id": "refkb:{}".format(result[0]['id']), 
                        "canonical_name": result[0]['CannonicalName'], 
                        'score': result[0]['confidence']})
            #             if result == 'none':
            #                 null_ents.append(ner)
            #         except:
            #             ner['link_lorelei'] = 'none'
            #             # print 'none'
            # # print(null_ents)
            # for null_ent in null_ents:
            #     result = tmpkb.query(null_ent)
            #     null_ent['link_lorelei'] = result
            # null_counter = defaultdict(int)
            # null_ents = filter(lambda x: x['link_lorelei'] == 'none', null_ents)
            # for null_ent in null_ents:
            #     null_counter[(null_ent['mention'].lower(), null_ent['type'][7:10])] += 1
            # for (name, type), count in null_counter.items():
            #     if count >= 5:
            #         tmpkb.register(name, type)
            # # tmpkb.register('test', 'test')
            
            with open(input_file, 'w') as f:
                json.dump(json_doc, f, indent=1, sort_keys=True)
    elif args.query:
        lucene.initVM(vmargs=['-Djava.awt.headless=true'])
        # linker = EntityLinker()
        linker = TemporaryKB()
        while True:
            name = raw_input('name:')
            ntype = raw_input('type:')
            ne = {'mention': name, 'type': 'ldcOnt:'+ntype}
            print linker.query(ne)
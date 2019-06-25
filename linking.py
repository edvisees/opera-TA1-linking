#!/usr/bin/env python2
# -*- coding: utf-8 -*-

import io
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
import csv

# nasty hack to fix "UnicodeDecodeError: ascii codec can't decode..."
# stackoverflow.com/questions/3828723/why-should-we-not-use-sys-setdefaultencodingutf-8-in-a-py-script
reload(sys)
sys.setdefaultencoding('utf8')


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
                info = '\t'.join([tokens[12], tokens[8], tokens[46]])
            elif src == 'WLL':
                info = '\t'.join([tokens[26], tokens[27], tokens[28]])
            elif src == 'APB':
                info = tokens[35]
            elif src == 'AIDA_AUG_PHASE1':
                info = ''
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
        
    def score_candidates(self, candidates, ent_name, ent_type):
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

        scores = [0 for _ in candidates]
        # find exact match
        for i, candidate in enumerate(candidates):
            # print candidate['name'].lower(), ent_name
            if candidate['name'].lower().encode('utf-8') == ent_name:
                scores[i] += 1
            elif ent_name in candidate['name'].lower().encode('utf-8'):
                scores[i] += 0.5

        # filter by type
        for i, candidate in enumerate(candidates):
            if candidate['type'] == ent_type:
                scores[i] += 1

        # filter by wiki
        for i, candidate in enumerate(candidates):
            if candidate['info'] == '': continue
            if len(candidate['info'].split('\t')) == 3: # candidate['info'].split('\t')[2] != '':
                scores[i] += 1

        # filter by country
        if ent_type == 'GPE' or ent_type == 'LOC':
            for i, candidate in enumerate(candidates):
                if candidate['info'] == '': continue
                if candidate['info'].split('\t')[1] == 'country,state,region,...':
                    scores[i] += 1
                if candidate['info'].split('\t')[0] == 'RU' or candidate['info'].split('\t')[0] == 'UA':
                    scores[i] += 1
                if candidate['info'].split('\t')[0] == 'US' or candidate['info'].split('\t')[0] == 'CA':
                    scores[i] -= 0.5

        max_score = -1
        final_candidates = None
        for candidate, score in zip(candidates, scores):
            if score > max_score:
                max_score = score
                final_candidates = [candidate]
            elif score == max_score:
                final_candidates.append(candidate)
        # print candidates, scores
        return final_candidates

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

        # filter by wiki
        filtered = filter(lambda x: x['info'].split('\t')[2] != '', candidates)
        if len(filtered) == 1:
            return filtered
        elif len(filtered) == 0:
            return None
        else:
            candidates = filtered

        # filter by country
        filtered = filter(lambda x: x['type'] != 'GPE' and x['type'] != 'LOC' or 
            x['info'].split('\t')[1] == 'country,state,region,...', candidates)
        if len(filtered) == 1:
            return filtered
        elif len(filtered) == 0:
            pass
        else:
            candidates = filtered
        filtered = filter(lambda x: x['type'] != 'GPE' and x['type'] != 'LOC' or 
            x['info'].split('\t')[0] == 'RU' or x['info'].split('\t')[0] == 'UA', candidates)
        if len(filtered) == 1:
            return filtered
        elif len(filtered) == 0:
            pass
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
        try:
            candidates = self.search_candidates(ent_name, 0)
        except:
            return 'none'
        # print candidates
        candidates = self.score_candidates(candidates, ent_name, ent_type)
        # print candidates
        if candidates is None or len(candidates) == 0:
            for dist in range(min(5, len(ent_name)//5)):
                try:
                    candidates = self.search_candidates(ent_name, dist+1)
                except:
                    return 'none'
                # print candidates
                candidates = self.score_candidates(candidates, ent_name, ent_type)
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
            # self.searcher = Searcher('tmp_index/')
        else:
            os.mkdir('tmp_index/')
            self.count = 0
            with open('tmp_index/count.txt', 'w') as f:
                f.write('{}'.format(self.count))
            # self.indexer = Indexer('tmp_index/')
            self.register('MH17', 'VEH')
            self.register('T-34', 'VEH')
            # self.searcher = Searcher('tmp_index/')

    def register(self, name, type):
        print 'registering:', name, type
        indexer = Indexer('tmp_index/')
        indexer.index('@{}'.format(self.count), name, name, type, '')
        self.count += 1
        with open('tmp_index/count.txt', 'w') as f:
            f.write('{}'.format(self.count))
        indexer.close()
        # print '$$', self.searcher.find_by_name(name)
        return '@{}'.format(self.count-1)

    def query(self, ne):
        try:
            ent_name, ent_type = ne['mention'].lower(), ne['type'][7:10]
            # print 'querying', ent_name, ent_type
            # print(ent_name, ent_type)
            searcher = Searcher('tmp_index/')
            results = searcher.find_by_name(ent_name)
            # print(results)
            results = filter(lambda x: x['type'] == ent_type, results)
            if results is None or len(results) == 0:
                return 'none'

            confsum = 0
            for result in results:
                score = 1. / (abs(len(result['name']) - len(ent_name)) + 1)
                result['confidence'] = score
                confsum += score
            for result in results:
                result['confidence'] /= confsum
            results.sort(key=lambda x: -x['confidence'])
            return results
        except:
            return 'none'

class WikiMapper(object):
    def __init__(self):
        self.mapping = {}
        with open('mapping_refkb2wiki.tab', 'r') as f:
            for line in f:
                eid, name, url = line.strip('\n').split('\t')
                if url != 'None':
                    self.mapping[eid] = url

    def map(self, eid):
        if eid in self.mapping:
            return self.mapping[eid]
        return None


def format_kb_id(kb_id):
    kb_prefix = "tmpkb" if '@' in kb_id else "refkb"
    return "{}:{}".format(kb_prefix, kb_id)


if __name__ == '__main__':
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument('--index', nargs='?', const="LDC2018E80_LORELEI_Background_KB", default=None, type=str,
                        help="clean and index reference KB (default dir: LDC2018E80_LORELEI_Background_KB)")
    parser.add_argument('--query', action='store_true')
    parser.add_argument('--query_tmp', action='store_true')
    parser.add_argument('--run', action='store_true')
    parser.add_argument('--run_csr', action='store_true')
    parser.add_argument('--en', action='store_true')
    parser.add_argument('--ru', action='store_true')
    parser.add_argument('--uk', action='store_true')
    parser.add_argument('--img', action='store_true')
    parser.add_argument('--in_dir', type=str)
    parser.add_argument('--out_dir', type=str)
    parser.add_argument('--map_file', type=str)
    args = parser.parse_args()

    if args.out_dir and not os.path.exists(args.out_dir):
        os.makedirs(args.out_dir)

    if args.index:
        data_cleaning(os.path.join(args.index, 'data/entities.tab'),
                        os.path.join(args.index, 'data/cleaned.tab'))
        lucene.initVM(vmargs=['-Djava.awt.headless=true'])
        os.system('rm -rf lucene_index/')
        indexer = Indexer('lucene_index/')
        for eid, name, cname, type, info in load_id2name(
                os.path.join(args.index, 'data/cleaned.tab'),
                os.path.join(args.index, 'data/alternate_names.tab')):
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
        wikimapper = WikiMapper()

        input_dir = args.in_dir
        for fname in os.listdir(input_dir):
            if not fname.endswith(".csr.json"):
                continue
            input_file = os.path.join(input_dir, fname)
            print input_file
            with open(input_file, 'r') as f:
                json_doc = json.load(f)

            ent_clusters = []
            for frame in json_doc['frames']:
                if frame['@type'] == 'relation_evidence' and frame['interp']['type'] == 'aida:entity_coreference':
                    cluster = []
                    for arg in frame['interp']['args']:
                        cluster.append(arg['arg'])
                    ent_clusters.append(cluster)
            # print ent_clusters

            if args.en:
                sentences = {}
                for frame in json_doc['frames']:
                    if frame['@type'] == 'sentence':
                        sentences[frame['@id']] = frame['provenance']['text']
                # print sentences

            id2entity = {}
            null_ents = set()
            linked_ents = {}

            for frame in json_doc['frames']:
                if frame['@type'] != 'entity_evidence':
                    continue
                id2entity[frame['@id']] = frame
                if 'form' not in frame['interp'] or frame['interp']['form'] != 'named':
                    continue
                if args.img:
                    text = frame['label'].encode('utf-8')
                else:
                    text = frame['provenance']['text'].encode('utf-8')
                enttype = frame['interp']['type']
                if type(enttype) == list:
                    enttype = enttype[0]['value']
                if args.en:
                    sent = sentences[frame['provenance']['reference']]
                    print text, enttype
                    ne = {'mention': text, 'type': enttype}
                    result = linker.query(ne, sent)
                elif args.ru or args.uk:
                    fringe = frame['interp']['fringe'] if 'fringe' in frame['interp'] else None
                    print text, enttype, fringe
                    ne = {'mention': text, 'type': enttype}
                    result = linker.query(ne, '')
                    if result != 'none' and not fringe is None:
                        fne = {'mention': fringe[1:], 'type': enttype}
                        fresult = linker.query(fne, '')
                        if fresult != 'none':
                            result_dict = dict([(ru_res['id'], ru_res) for ru_res in result])
                            for en_res in fresult:
                                if en_res['id'] in result_dict:
                                    newscore = en_res['confidence'] + result_dict[en_res['id']]['confidence']
                                    newscore = min(1.0, newscore)
                                    result_dict[en_res['id']]['confidence'] = newscore
                                else:
                                    result_dict[en_res['id']] = en_res
                            result = list(result_dict.values())
                            result.sort(key=lambda x: -x['confidence'])
                elif args.img:
                    print text, enttype
                    ne = {'mention': text, 'type': enttype}
                    result = linker.query(ne, '')

                if result != 'none':
                    print result
                    if 'xref' not in frame['interp']:
                        frame['interp']['xref'] = []
                    frame['interp']['xref'] = filter(lambda x: x['component'] != "opera.entities.edl.refkb.xianyang", frame['interp']['xref'])
                    if any(x['id'].startswith("refkb:") and x['component'] != "opera.entities.edl.refkb.xianyang" for x in frame['interp']['xref']):
                        continue
                    frame['interp']['xref'].append({"@type": "db_reference", 
                        "component": "opera.entities.edl.refkb.xianyang",
                        "id": format_kb_id(result[0]['id']),
                        "canonical_name": result[0]['CannonicalName'], 
                        'score': result[0]['confidence'], 'subcomponent': 0})
                    wiki_link = wikimapper.map(result[0]['id'])
                    if wiki_link:
                        frame['interp']['xref'].append({"@type": "db_reference", 
                        "component": "opera.entities.edl.wikipedia.xianyang",
                        "id": wiki_link, 
                        'score': result[0]['confidence']})
                    
                else:
                    null_ents.add(frame['@id'])

            for null_ent in null_ents:
                frame = id2entity[null_ent]
                if args.img:
                    text = frame['label'].encode('utf-8')
                else:
                    text = frame['provenance']['text'].encode('utf-8')
                enttype = frame['interp']['type']
                if type(enttype) == list:
                    enttype = enttype[0]['value']
                ne = {'mention': text, 'type': enttype}
                result = tmpkb.query(ne)
                if result != 'none':
                    print '****', result
                    if 'xref' not in frame['interp']:
                        frame['interp']['xref'] = []
                    frame['interp']['xref'] = filter(lambda x: x['component'] != "opera.entities.edl.refkb.xianyang", frame['interp']['xref'])
                    if any(x['id'].startswith("refkb:") and x['component'] != "opera.entities.edl.refkb.xianyang" for x in frame['interp']['xref']):
                        continue
                    frame['interp']['xref'].append({"@type": "db_reference", 
                        "component": "opera.entities.edl.refkb.xianyang",
                        "id": format_kb_id(result[0]['id']),
                        "canonical_name": result[0]['CannonicalName'], 
                        'score': result[0]['confidence'], 'subcomponent': 1})
                    # wiki_link = wikimapper.map(result[0]['id'])
                    # if wiki_link:
                    #     frame['interp']['xref'].append({"@type": "db_reference", 
                    #     "component": "opera.entities.edl.wikipedia.xianyang",
                    #     "id": wiki_link, 
                    #     'score': result[0]['confidence']})

            for coref_cluster in ent_clusters:
                linked_ents = []
                for eid in coref_cluster:
                    frame = id2entity[eid]
                    if 'xref' in frame['interp']:
                        linked = None
                        for link_res in frame['interp']['xref']:
                            if link_res['component'] == "opera.entities.edl.refkb.xianyang":
                                linked = link_res
                                break
                        if not linked is None:
                            linked_ents.append(linked)
                # print(linked_ents)
                
                if len(linked_ents) == 0:
                    # register new KB entry
                    mention_counter = defaultdict(int)
                    for eid in coref_cluster:
                        frame = id2entity[eid]
                        if 'form' not in frame['interp'] or frame['interp']['form'] != 'named':
                            continue
                        mention = frame['provenance']['text']
                        mention_counter[mention] += 1
                    # print mention_counter
                    best_mention = None
                    max_count = 0
                    for mention, count in mention_counter.items():
                        if count > max_count:
                            max_count = count
                            best_mention = mention
                        elif count == max_count:
                            if len(mention) > len(best_mention):
                                best_mention = mention
                    if not best_mention is None:
                        for eid in coref_cluster:
                            if id2entity[eid]['provenance']['text'] == best_mention:
                                enttype = id2entity[eid]['interp']['type']
                                break
                        if type(enttype) == list:
                            enttype = enttype[0]['value']
                        enttype = enttype[7:10]
                        if enttype in ['GPE', 'LOC', 'FAC', 'PER', 'ORG', 'VEH', 'WEA']:
                            for eid in coref_cluster:
                                print '!', id2entity[eid]
                            tid = tmpkb.register(best_mention.lower(), enttype)
                            print tmpkb.query({'mention': best_mention, 'type': 'ldcOnt:'+enttype})
                            for eid in coref_cluster:
                                frame = id2entity[eid]
                                if 'xref' not in frame['interp']:
                                    frame['interp']['xref'] = []
                                frame['interp']['xref'] = filter(lambda x: x['component'] != "opera.entities.edl.refkb.xianyang", frame['interp']['xref'])
                                if any(x['id'].startswith("refkb:") and x['component'] != "opera.entities.edl.refkb.xianyang" for x in frame['interp']['xref']):
                                    continue
                                frame['interp']['xref'].append({"@type": "db_reference", 
                                "component": "opera.entities.edl.refkb.xianyang",
                                "id": format_kb_id(tid),
                                "canonical_name": best_mention, 
                                'score': 1.0, 'subcomponent': 2})
                else:
                    # coreferent mentions should be linked to the same entity
                    votes = defaultdict(float)
                    for linked in linked_ents:
                        votes[linked['id']] += linked['score']
                    max_vote = 0
                    for eid, vote_score in votes.items():
                        if vote_score > max_vote:
                            max_vote = vote_score
                            votedid = eid
                    votedwiki = wikimapper.map(votedid)
                    for linked in linked_ents:
                        if linked['id'] == votedid:
                            final_linking = linked
                            break
                    for eid in coref_cluster:
                        frame = id2entity[eid]
                        if 'xref' in frame['interp']:
                            frame['interp']['xref'] = filter(lambda x: x['component'] != "opera.entities.edl.refkb.xianyang", frame['interp']['xref'])
                            if any(x['id'].startswith("refkb:") and x['component'] != "opera.entities.edl.refkb.xianyang" for x in frame['interp']['xref']):
                                continue
                            frame['interp']['xref'].append(final_linking)
                        else:
                            frame['interp']['xref'] = [final_linking]
                        if votedwiki:
                            frame['interp']['xref'].append({"@type": "db_reference", 
                                "component": "opera.entities.edl.wikipedia.xianyang",
                                "id": votedwiki, 
                                'score': result[0]['confidence']})
            
            # with open(os.path.join(args.out_dir, fname), 'w') as f:
            #     json.dump(json_doc, f, indent=1, sort_keys=True)
            with io.open(os.path.join(args.out_dir, fname), 'w', encoding='utf8') as f:
                f.write(unicode(json.dumps(json_doc, indent=1, sort_keys=True, ensure_ascii=False)))
    # elif args.run_csr_ru:
    #     lucene.initVM(vmargs=['-Djava.awt.headless=true'])
    #     linker = EntityLinker()
    #     tmpkb = TemporaryKB()
    #     input_dir = args.dir
    #     for fname in os.listdir(input_dir):
    #         input_file = os.path.join(input_dir, fname)
    #         print input_file
    #         with open(input_file, 'r') as f:
    #             json_doc = json.load(f)
    #         null_ents = []
    #         for frame in json_doc['frames']:
    #             if frame['@type'] != 'entity_evidence':
    #                 continue
    #             text = frame['provenance']['text'].encode('utf-8')
    #             type = frame['interp']['type']
    #             fringe = frame['interp']['fringe'] if 'fringe' in frame['interp'] else None
    #             print text, type, fringe
    #             ne = {'mention': text, 'type': type}
    #             result = linker.query(ne, '')
    #             if not fringe is None:
    #                 fne = {'mention': text, 'type': type}
    #                 fresult = linker.query(fne, '')
    #             if result != 'none':
    #                 print result
    #                 if 'xref' not in frame['interp']:
    #                     frame['interp']['xref'] = []
    #                 frame['interp']['xref'].append({"@type": "db_reference", 
    #                     "component": "opera.entities.edl.refkb.xianyang",
    #                     "id": format_kb_id(result[0]['id']),
    #                     "canonical_name": result[0]['CannonicalName'], 
    #                     'score': result[0]['confidence']})
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
            
            # with open(input_file, 'w') as f:
            #     json.dump(json_doc, f, indent=1, sort_keys=True)
    elif args.query:
        lucene.initVM(vmargs=['-Djava.awt.headless=true'])
        linker = EntityLinker()
        while True:
            name = raw_input('name:')
            ntype = raw_input('type:')
            ne = {'mention': name, 'type': 'ldcOnt:'+ntype}
            print linker.query(ne, '')
    elif args.query_tmp:
        lucene.initVM(vmargs=['-Djava.awt.headless=true'])
        linker = TemporaryKB()
        while True:
            name = raw_input('name:')
            ntype = raw_input('type:')
            ne = {'mention': name, 'type': 'ldcOnt:'+ntype}
            print linker.query(ne)
    elif args.map_file:
        lucene.initVM(vmargs=['-Djava.awt.headless=true'])
        linker = EntityLinker()

        if 'named_gpe' in args.map_file:
            enttype = 'ldcOnt:GPE'
        elif 'named_people' in args.map_file:
            enttype = 'ldcOnt:PER'
        with open(args.map_file, 'r') as f:
            reader = csv.reader(f, delimiter=',')
            for row in reader:
                if row[0] != 'L':
                    continue
                name = row[1][1:]
                concept = row[2][1:]
                result = linker.query({'mention': name, 'type': enttype}, '')
                if result == 'none':
                    print u'{}\t{}\t{}'.format(name.decode('utf-8'), concept.decode('utf-8'), 'none').encode('utf-8')
                else:
                    out = u'{}\t{}'.format(name.decode('utf-8'), concept.decode('utf-8'))
                    for r_id, refkb_entry in enumerate(result):
                        # if r_id == 3:
                        #     break
                        refkbid = refkb_entry['id']
                        refkbname = refkb_entry['CannonicalName']
                        if refkb_entry['info'] == '':
                            print(out + '\t{}\t{}'.format(refkbid, refkbname)).encode('utf-8')
                        else:
                            if enttype == 'ldcOnt:GPE':
                                info = refkb_entry['info'].split('\t')
                                country, feature, link = info
                                print(out + u'\t{}\t{}\t{}\t{}\t{}'.format(refkbid, refkbname, country, feature, link)).encode('utf-8')
                            elif enttype == 'ldcOnt:PER':
                                info = refkb_entry['info'].split('\t')
                                country = info[0]
                                title = info[1]
                                org = info[2]
                                print(out + u'\t{}\t{}\t{}\t{}\t{}'.format(refkbid, refkbname, country, title, org)).encode('utf-8')
                    # print out.encode('utf-8')

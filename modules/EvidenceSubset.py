import simplejson as json
import os
import yaml
import datetime
from opentargets_urlzsource import URLZSource
from common import make_gzip
import logging

logger = logging.getLogger(__name__)

class EvidenceSubset(object):

    def __init__(self, filename, output_dir, gs_output_dir):
        self.suffix = datetime.datetime.today().strftime('%Y-%m-%d')
        self.filename_subset_evidence = filename
        self.output_dir = output_dir
        self.gs_output_dir = gs_output_dir+'/subsets'
        self.stats = {}
        self.elem_to_search = set()
        self.read_subset_file()


    def deref_multi(self,data, keys):
        return self.deref_multi(data[keys[0]], keys[1:]) \
            if keys else data


    def create_subset(self,evidence_file, evidence_info):
        count = 0
        path_filename, filename_attr = os.path.split(evidence_file)
        new_filename = "subset_" + filename_attr.replace('.gz', '')
        uri_to_filename = self.output_dir + '/' + new_filename
        if os.path.exists(uri_to_filename): os.remove(uri_to_filename)
        self.stats[evidence_file]['ensembl'] = {}
        with open(uri_to_filename, "a+") as file_subset:
            with URLZSource(evidence_file).open() as f_obj:
                for line in f_obj:
                    try:
                        read_line = json.loads(line)
                        new_key = self.deref_multi(read_line, evidence_info['subset_key'])
                        new_key = new_key.replace(evidence_info['subset_prefix'],'')
                        count = count + 1
                        if new_key in self.elem_to_search:
                            file_subset.write(line)
                            if new_key not in self.stats[evidence_file]['ensembl']:
                                self.stats[evidence_file]['ensembl'][new_key] = 1
                            else:
                                self.stats[evidence_file]['ensembl'][new_key]= self.stats[evidence_file]['ensembl'][new_key] + 1

                    except Exception as e:
                        logging.info("This line is not in a JSON format. Skipped it")

            self.stats[evidence_file]['num_key'] = count
        logging.debug("Finished")
        return uri_to_filename

    def read_subset_file(self):
        with URLZSource(self.filename_subset_evidence).open() as f_obj:
            for line in f_obj:
                self.elem_to_search.add(line.rstrip('\n'))
        logging.debug(self.elem_to_search)


    def create_stats_file(self):
        with open(self.output_dir+'/stats_subset_files.yml', 'w') as outfile:
            yaml.dump(self.stats, outfile, default_flow_style=False)

    def execute_subset(self, evidences_list):
        list_files_subset_evidence = {}
        for evidence_file in evidences_list:
            logging.info("Start process for the file {}".format(evidence_file))
            self.stats[evidence_file] = {}
            if evidences_list[evidence_file]['subset_key'] is not None:
                subset_file = self.create_subset(evidence_file, evidences_list[evidence_file])
                filename_zip = make_gzip(subset_file)
                list_files_subset_evidence[filename_zip] = {'resource': 'subset_evidence', 'gs_output_dir': self.gs_output_dir}
                self.stats[evidence_file]['filename'] = filename_zip
                logging.info("File {} has been created".format(filename_zip))
            else:
                self.stats[evidence_file]['filename'] = "The file {} won't have subset evidence file.".format(evidence_file)
                logger.info("The file {} won't have subset evidence file.".format(evidence_file))

        return list_files_subset_evidence


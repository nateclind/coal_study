import os
import pathlib
import glob
import re
import psycopg2
from contextlib import contextmanager
import logging
from logging import basicConfig, exception, WARNING


class ProquestParser():
   
    @staticmethod
    def get_text(line, infile):
        """[Gets text from ProQuest Newspaper data]
        
        Arguments:
            line {[str]} -- [Current infile line]
            infile {[obj]} -- [Read-only .txt file with encoding='Windows-1252']
        
        Returns:
            [str] -- [Returns text]
        """
        doc_text = re.compile(r'(Full Text \n)|(Full text \n)')
        text_search = doc_text.search(line)

        if text_search:
            doc_end = re.compile(r'(Graph\s*\n)|(Photograph\s*\n)|(Illustration\s*\n)|(Details\s*\n)|(Subject\s*)|(Copyright New York Times\s*)|(Copyright USA Today\s*)|(Copyright \(c\))\s*|(\(c\) [0-9]+)|(Credit: By\s*)')
            end_search = None
            text = ''

            while not end_search:
                line = infile.readline() 
                end_search = doc_end.search(line)
                
                if not end_search:
                    text += line.strip('\n')

        return text
    
    @staticmethod
    def get_id(line):
        """[Gets document ID from ProQuest Newspaper data]
        
        Arguments:
            line {[str]} -- [Current infile line]
        
        Returns:
            [str] -- [Returns ID]
        """
        doc_id = re.compile(r'(.*?)(/docview/)([0-9]+)(.*?)')
        pqid_search = doc_id.search(line)

        if pqid_search:
            pq_id = pqid_search.group(3)
        
        return pq_id


@contextmanager
def incoming(input_path):
    """[Opens read-only .txt file with encoding='Windows-1252']
    
    Arguments:
        input_path {[str]} -- [Input path]
    
    Yields:
        [obj] -- [Read-only .txt file with encoding='Windows-1252']
    """
    try:
        infile = open(input_path, 'r', encoding='Windows-1252') 
    except FileNotFoundError as incoming_error:
        exception(incoming_error)
    except Exception as incoming_e:
        exception(incoming_e)
    else:
        yield infile
    finally:
        infile.close()

@contextmanager
def outgoing(cred):
    """[Opens DB connection with PostgreSQL host='localhost']
    
    Arguments:
        cred {[str]} -- [Credentials]
    
    Yields:
        [obj] -- [DB connection with PostgreSQL host='localhost']
    """
    try:
        outfile = psycopg2.connect(user='postgres', password=cred[1], host='localhost', port='5432', database='postgres')
    except ConnectionError as outgoing_error:
        exception(outgoing_error)
    except Exception as outgoing_e:
        exception(outgoing_e)
    else:
        yield outfile
    finally:
        outfile.close()


def main():
    """[Driver of news_parser.py]

    Setup:
        pp {[cls]} -- [ProquestParser() instance]
        cred {[str]} -- [Credentials]
        root_path {[str]} -- [Root path]
        news_path {[str]} -- [Local path]
    """
    pp = ProquestParser()
    cred = pathlib.Path('login.bin').read_text().split('|')
    root_path = os.path.dirname(__file__)
    news_path = os.path.join(root_path, 'data', 'news')
    basicConfig(filename='np.log', filemode='w', format='%(asctime)s %(message)s', datefmt='%m/%d/%Y %I:%M:%S %p', level=WARNING)

    pq_id, text = None, None

    for filename in glob.glob(os.path.join(news_path, '*.txt')):

        input_path = filename

        with incoming(input_path) as infile, outgoing(cred) as outfile:
            outfile_curs = outfile.cursor()
            line = infile.readline()

            while line:
                line = infile.readline()

                try:
                    text = pp.get_text(line, infile)
                except:
                    pass

                try:
                    pq_id = pp.get_id(line)
                except:
                    pass

                if None not in (pq_id, text):
                    outfile_curs.execute('INSERT INTO news_tbl (doc_id , doc_text) VALUES (%s, %s);', (pq_id, text))
                    outfile.commit()
                    pq_id, text = None, None

        outfile_curs.close()
    logging.shutdown()

if __name__ == '__main__':
    main()

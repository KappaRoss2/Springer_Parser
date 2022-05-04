import logging
import requests
import csv
import bs4
import collections
import concurrent.futures as pool
import requests.adapters


logging.basicConfig(level = logging.DEBUG)
logger = logging.getLogger('log')

ParseResult = collections.namedtuple(
    'ParseResult',
    (
        'Article',
        'Reference',
        'Author',

    ),
)

class parser:


    def __init__(self, key):

        self.session = requests.Session()
        self.adapter = requests.adapters.HTTPAdapter(pool_connections=80, pool_maxsize=70)
        self.session.mount('http://', self.adapter)

        self.key = key
        self.result = []


    def load_page(self, element = 1):
        url = f'http://api.springernature.com/metadata/pam?q=type:Journal&s={element}&p=50&api_key='+ self.key
        res = self.session.get(url = url)
        res.raise_for_status()
        return res.text


    def parse_page(self, text: str):

        soup = bs4.BeautifulSoup(text, 'xml')
        container = soup.find_all('pam:article')
        for block in container:
            self.parse_block(block = block)


    def get_last_element(self):

        text = self.load_page()
        soup = bs4.BeautifulSoup(text, "xml")
        last_page = soup.find('total').text

        return last_page


    def parse_block(self, block):

        article = block.find('dc:title').text
        url = block.find('prism:url').text

        creators_temp = block.findAll('dc:creator')
        creators = list()

        for el in creators_temp:
            el = el.text
            temp = el.split(",")
            temp.reverse()
            el = ' '.join(temp)
            creators.append(el)
        creators = ','.join(creators)

        self.result.append(ParseResult (
            Article = article,
            Reference = url,
            Author = creators
        ))


    def save_result(self):

        with open('springer.csv', 'a', encoding="utf-8") as f:
            writer = csv.writer(f, quoting=csv.QUOTE_MINIMAL)
            for el in self.result:
                writer.writerow(el)

        self.result.clear()


    def run(self, elements):

        with pool.ThreadPoolExecutor(max_workers=70) as executer:
            res = executer.map(self.load_page, elements)
            parse_page = executer.map(self.parse_page, res)

        self.save_result()
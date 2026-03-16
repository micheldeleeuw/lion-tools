from html.parser import HTMLParser

class RowExtractor(HTMLParser):
    def __init__(self):
        super().__init__()
        self.in_tbody = False
        self.row_idx = 0
        self.current_row_tds = []
        self.in_td = False
        self.td_content = ''
        self.target_row = None
        
    def handle_starttag(self, tag, attrs):
        if tag == 'tbody':
            self.in_tbody = True
        elif tag == 'tr' and self.in_tbody:
            self.current_row_tds = []
        elif tag == 'td' and self.in_tbody:
            self.in_td = True
            self.td_content = ''
            
    def handle_endtag(self, tag):
        if tag == 'tbody':
            self.in_tbody = False
        elif tag == 'tr' and self.in_tbody:
            self.row_idx += 1
            if self.row_idx == 11:
                self.target_row = list(self.current_row_tds)
        elif tag == 'td' and self.in_tbody:
            self.in_td = False
            self.current_row_tds.append(self.td_content)
            
    def handle_data(self, data):
        if self.in_td:
            self.td_content += data

content = open('output/x.html').read()
ext = RowExtractor()
ext.feed(content)

if ext.target_row:
    print(f'Row 11 has {len(ext.target_row)} cells')
    # Show non-empty cells
    for i, val in enumerate(ext.target_row):
        if val.strip():
            print(f'  [{i}] = {repr(val[:100])}')
else:
    print('Row 11 not found')

import pandas as pd

class PointPath():

    def __init__(self, **kwargs):

        if "from_csv" in kwargs:
            path = kwargs.get("from_csv")

            with open(path, 'r') as csv_file:
                tag_str = csv_file.readlines(1)

            tag_str = tag_str[0]

            self.tags = {}

            for tag in tag_str.split(','):
                print(tag)
                if '#' in tag:
                    pass
                elif '=' in tag:
                    tag_key, tag_val = tag.replace('\n','').split('=')
                    self.tags.update({tag_key : tag_val})
                else:
                    pass

            self.path_df = pd.read_csv(path, 
                                       comment='#')


        self.lats = self.path_df['lat']
        self.lons = self.path_df['lon']
        self.alts = self.path_df['alt']
        self.names = self.path_df['name']

        if "time_mode" in self.tags:
            time_mode = self.tags.get("time_mode")
            if time_mode == 'comp':
                raise NotImplementedError
            if time_mode == 'str':
                self.times = self.path_df['time']

    def __get__(self, instance, owner):
        return self.value

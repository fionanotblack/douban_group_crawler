# douban_group_crawler
An automation crawler to collect posts, comments and basic members information of a selected group in douban.com

# How to use
## Crawler data
Change these two lines in crawler_db.py to your group link:

group_url = 'https://www.douban.com/group/lala/discussion?start=0'

group_memebers_url = 'https://www.douban.com/group/lala/members'


## Analyze data
Use ipython notebook for file User Behavior Analysis of Douban.com.ipynb


# Environment
Python 2.7+

BeautifulSoup Lib

Pandas and Numpy Lib

ipython

Note: Anaconda package is recommended
# -*- coding: UTF-8 -*-
from bs4 import BeautifulSoup
import urllib2,csv,datetime,time
import cookielib
import random
import math
import os

group_url = 'https://www.douban.com/group/lala/discussion?start=0'
group_memebers_url = 'https://www.douban.com/group/lala/members'
PAGE_SIZE = 25 # in group homepage, each page contains at most 25 posts
REPLY_SIZE = 100 # in post page, each page contains at most 100 comments
MEMEBER_SIZE = 35
BATCH_CRAWL_PAGE_MAX_CNT = 3

special_user_id = '54885196' # the user you want to collect his/her comments
special_comments_f = open('special_comments.txt', 'ab')

def refresh_cookie():
    cj = cookielib.CookieJar()
    opener = urllib2.build_opener(urllib2.HTTPCookieProcessor(cj))
    urllib2.install_opener(opener)

def collect_posts():
    page = urllib2.urlopen(group_url).read()
    # with open("page.html","wb") as f:
    #     f.write(page)
    soup = BeautifulSoup(page, "lxml")
    paginator = soup.select('.paginator a')
    last_page_link = paginator[-2].get('href')
    end_idx = int(last_page_link.split('=')[1])
    # print last_page_link,last_page_idx

    base_url = group_url[:-1]

    if os.path.isfile('posts.csv'):
        start_idx = len(open("posts.csv").readlines()) - 1
    else:
        start_idx = 0
        with open("posts.csv",'wb') as f:
            header = ['title','author','replies','last_reply_time','href']
            spamwriter = csv.DictWriter(f, fieldnames=header, delimiter=',',
                                    quotechar='"', quoting=csv.QUOTE_MINIMAL)
            spamwriter.writeheader()

    idx = start_idx
    with open("posts.csv",'ab') as f:
        header = ['title','author','replies','last_reply_time','href']
        spamwriter = csv.DictWriter(f, fieldnames=header, delimiter=',',
                                    quotechar='"', quoting=csv.QUOTE_MINIMAL)

        batch_crawl_page_cnt = 0
        while idx <= end_idx:
            refresh_cookie()
            page_url = base_url+str(idx)
            print datetime.datetime.now()
            print "Start to crawler %s" % page_url

            page = urllib2.urlopen(page_url).read().decode('utf-8')
            soup = BeautifulSoup(page, "lxml")
            batch_crawl_page_cnt += 1
            rows = soup.select('.olt tr')
            for r in rows:
                post_info = {}
                title = r.find('td', {"class":"title"})

                if title is None:
                    continue
                # print title.encode('utf-8')
                post_info['href'] = title.find('a').get('href')
                post_info['title'] = title.find('a').get('title').encode('utf-8')

                author_link = r.find_all('td', {"nowrap":"nowrap"})[0].find('a').get('href')
                # print author_link
                post_info['author'] = author_link.split('/')[-2]
                post_info['replies'] = r.find_all('td', {"nowrap":"nowrap"})[1].text
                post_info['last_reply_time'] = r.find_all('td', {"nowrap":"nowrap"})[2].text
                spamwriter.writerow(post_info)

            idx += PAGE_SIZE
            if batch_crawl_page_cnt >= BATCH_CRAWL_PAGE_MAX_CNT:
                sleep_time = random.randint(1,10)
                print "Sleep %d secs to avoid to be blocked." % sleep_time
                time.sleep(sleep_time)
                batch_crawl_page_cnt = 0

def collect_replies():
    with open('posts.csv') as csvfile:
        reader = csv.DictReader(csvfile)
        batch_post_cnt = 0
        post_cnt = 0

        if os.path.isfile('post_count.txt'):
            with open('post_count.txt') as f:
                current_post_cnt = int(f.read())
        else:
            current_post_cnt = 0
            with open("comments.csv",'ab') as f:
                header = ['post_id','user_id','comment_id','pub_time']
                spamwriter = csv.DictWriter(f, fieldnames=header, delimiter=',',
                                    quotechar='"', quoting=csv.QUOTE_MINIMAL)
                spamwriter.writeheader()
        for row in reader:
            post_cnt += 1
            if post_cnt <= current_post_cnt:
                continue
            print "Current post num:%d" % post_cnt
            post_url = row['href']
            collect_replies_for_post(post_url)
            try:
                post_page_num = int(row['replies'])
            except:
                post_page_num = BATCH_CRAWL_PAGE_MAX_CNT
            # print "post_page_num:%d" % post_page_num
            post_page_num = float(post_page_num)
            batch_post_cnt += math.ceil(post_page_num/REPLY_SIZE)
            # print batch_post_cnt, math.ceil(post_page_num/REPLY_SIZE)
            if batch_post_cnt >= BATCH_CRAWL_PAGE_MAX_CNT:
                refresh_cookie()
                sleep_time = random.randint(1,10)
                print "Sleep %d secs to avoid to be blocked." % sleep_time
                time.sleep(sleep_time)
                batch_post_cnt = 0

            with open('post_count.txt', 'wb') as f:
                f.write(str(post_cnt))

def collect_replies_for_post(post_url):
    try:
        page = urllib2.urlopen(post_url).read().decode('utf-8')
        soup = BeautifulSoup(page, "lxml")
        paginator = soup.find('div',{"class":'paginator'})

        page_num = 1
        if paginator:
            page_num = int(paginator.find('span',{"class":'thispage'}).get("data-total-page"))
        print "This post has %d pages" % page_num

        collect_replies_for_page(post_url)
        batch_crawl_page_cnt = 0
        for page_idx in range(2, page_num + 1):
            page_url = post_url+'?start='+str((page_idx-1)*REPLY_SIZE)
            collect_replies_for_page(page_url)

            batch_crawl_page_cnt += 1
            if batch_crawl_page_cnt >= BATCH_CRAWL_PAGE_MAX_CNT:
                sleep_time = random.randint(5,10)
                print "Sleep %d secs to avoid to be blocked." % sleep_time
                time.sleep(sleep_time)
                batch_crawl_page_cnt = 0
    except urllib2.HTTPError, err:
        if err.code == 404:
            pass
        else:
            raise

def collect_replies_for_page(page_url):
    print "Start to crawler %s" % page_url
    post_id = page_url.split('/')[-2]

    start_access_time = time.time()
    page = urllib2.urlopen(page_url).read().decode('utf-8')
    end_access_time = time.time()
    print "Get this page in %f s." % (end_access_time - start_access_time)
    soup = BeautifulSoup(page, "lxml")
    with open("comments.csv",'ab') as f:
        header = ['post_id','user_id','comment_id','pub_time']
        spamwriter = csv.DictWriter(f, fieldnames=header, delimiter=',',
                                    quotechar='"', quoting=csv.QUOTE_MINIMAL)
        # spamwriter.writeheader()
        comment_items = soup.select("#comments .comment-item")
        comments = []
        for item in comment_items:
            comment = {"post_id":post_id}
            comment['comment_id'] = item.get('id')
            item_bg = item.find('div',{'class':'bg-img-green'})
            # print item_bg
            comment['user_id'] = item_bg.find('a').get('href').split('/')[-2]
            comment['pub_time'] = item_bg.find('span',{"class":"pubtime"}).text
            # spamwriter.writerow(comment)
            comments.append(comment)

            # collect comments for the special user
            if comment['user_id'] == special_user_id:
                special_comments_f.write("@Post %s"+post_id)
                special_comments_f.write(item_bg.text.encode('utf-8'))
                reply_quote = item.find('div',{"class":"reply-quote"})
                if reply_quote:
                    special_comments_f.write("Reply:"+reply_quote.find('span',{"class":"all"}).text.encode('utf-8'))
                    special_comments_f.write('\n\n')
                content = item.find('p')
                special_comments_f.write(content.text.encode('utf-8'))
        spamwriter.writerows(comments)

def collect_members():

    page = urllib2.urlopen(group_memebers_url).read()
    # with open("page.html","wb") as f:
    #     f.write(page)
    soup = BeautifulSoup(page, "lxml")
    paginator = soup.select('.paginator a')
    last_page_link = paginator[-2].get('href')
    last_page_idx = int(last_page_link.split('=')[1])
    print last_page_idx


    if os.path.isfile('members.csv'):
        start_idx = len(open("members.csv").readlines()) - 1
    else:
        start_idx = 0
    print start_idx
    if start_idx == 0:
        with open("members.csv",'wb') as f:
            header = ['user_id','username','location']
            spamwriter = csv.DictWriter(f, fieldnames=header, delimiter=',',
                                    quotechar='"', quoting=csv.QUOTE_MINIMAL)
            spamwriter.writeheader()
    idx = start_idx
    batch_crawl_page_cnt = 0
    while idx <= last_page_idx:
        refresh_cookie()
        member_url = group_memebers_url+'?start='+str(idx)
        collect_memebers_for_page(member_url)
        batch_crawl_page_cnt += 1
        idx += MEMEBER_SIZE
        if batch_crawl_page_cnt >= BATCH_CRAWL_PAGE_MAX_CNT:
            sleep_time = random.randint(1,10)
            print "Sleep %d secs to avoid to be blocked." % sleep_time
            time.sleep(sleep_time)
            batch_crawl_page_cnt = 0


def collect_memebers_for_page(member_url):
    print "Start to crawler %s" % member_url
    start_access_time = time.time()
    page = urllib2.urlopen(member_url).read().decode('utf-8')
    soup = BeautifulSoup(page, "lxml")
    end_access_time = time.time()
    print "Get this page in %f s." % (end_access_time - start_access_time)
    with open("members.csv",'ab') as f:
        header = ['user_id','username','location']
        spamwriter = csv.DictWriter(f, fieldnames=header, delimiter=',',
                                    quotechar='"', quoting=csv.QUOTE_MINIMAL)
        # spamwriter.writeheader()
        members = soup.select('.member-list li')
        for m in members:
            member_info = {}
            name_ele = m.find('div',{"class":"name"})
            member_info['user_id'] = name_ele.find('a').get('href').split('/')[-2]
            member_info['username'] = name_ele.find('a').text.encode('utf-8')
            member_info['location'] = name_ele.find('span',{'class':'pl'}).text.encode('utf-8').replace('(','').replace(')','')
            spamwriter.writerow(member_info)

if __name__ == '__main__':
    start_time = datetime.datetime.now()
    #################################################
    # Step 1: Catch all the posts in setting group  #
    # Uncomment the following line to run this step #
    #################################################

    collect_posts()

    #################################################
    # Step 2: Catch all the comments for each posts #
    # Uncomment the following line to run this step #
    # This step depends on step 1                   #
    #################################################

    # collect_replies()

    #################################################
    # Step 3: Catch all the members in this group   #
    # Uncomment the following line to run this step #
    # This step has no dependency                   #
    #################################################

    # collect_members()
    print "Finished in %f seconds." % int(datetime.datetime.now() - start_time)
import asyncio

import vk_api
import itertools
import psycopg2


conn = psycopg2.connect(
    database='postgres',
    user='postgres',
    password='********',
    host='db-course.csvgsji3hkkk.us-east-1.rds.amazonaws.com',
    port=5432,
)

vk_session = vk_api.VkApi(token='ca8f2cf5ca8f2cf5ca8f2cf584caf9957acca8fca8f2cf5aace3cdd64956b1ec10bd0d3')
vk = vk_session.get_api()
domain = 'itis_kfu'


def get_top_words(vk, domain):
    top_words = {}
    cur = conn.cursor()

    for i in range(2):
        data = vk.wall.get(domain=domain, offset=i * 100, count=100)
        for item in data['items']:
            text = item['text']
            text: str
            words = text.lower().split(" ")
            for word in words:
                if word in top_words:
                    top_words[word] += 1
                else:
                    top_words[word] = 0
    # print(top_words)
    sorted_x = sorted(top_words.items(), key=lambda item: item[1])
    sorted_x.reverse()
    new_top = {k: v for k, v in sorted_x}

    new_top = dict(itertools.islice(new_top.items(), 100))
    return new_top


def db(new_top):
    cur = conn.cursor()
    cur.execute('''CREATE TABLE IF NOT EXISTS WORDS
             (ID SERIAL PRIMARY KEY,
             WORD VARCHAR(200) NOT NULL,
             NUM INT);''')
    cur.execute('TRUNCATE TABLE WORDS')
    print("table created")

    sorted_dict = dict(sorted(new_top.items(), key=lambda item: item[1], reverse=True))

    for w, num in sorted_dict.items():
        cur.execute("INSERT INTO words (WORD, NUM) VALUES ('" + w + "' ," + str(num) + ");")
        conn.commit()

    print("data inserted")

    conn.close()


db(get_top_words(vk, domain))


# async def worker():
#     await database.preapare_db()
#     print('db ready')
#     vk_session = vk_api.VkApi(token='ca8f2cf5ca8f2cf5ca8f2cf584caf9957acca8fca8f2cf5aace3cdd64956b1ec10bd0d3')
#     vk = vk_session.get_api()
#     domain = 'itis_kfu'
#
#     top_words = get_top_words(vk, domain)
#     print('all words got')
#     await database.TopWords.clear_table()
#     for k, v in top_words.items():
#         await database.TopWords.create(k, v)
#     print('finish')


# asyncio.run(worker())
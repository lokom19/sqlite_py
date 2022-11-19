#!/usr/bin/env python
# coding: utf-8

# ### Создание и заполнение базы данных

# 1\. Создайте файл БД sqlite3 согласно рисунку ниже, на котором определен набор таблиц и связей между ними. Обратите внимание, что поля, выделенные полужирным шрифтом, обозначают первичный ключ таблицы.
# 
# Для решения задания напишите скрипт на языке SQL и исполните его при помощи метода `executescript` объекта-курсора.

# In[242]:


conn = sqlite3.connect('first_db.db')


# In[243]:


cur = conn.cursor()


# In[285]:


cur.execute("""CREATE TABLE IF NOT EXISTS review(
   id INT PRIMARY KEY,
   user_id INT,
   recipe_id INT,
   date VARCHAR,
   rating INT,
   review TEXT);
""")
conn.commit()


# In[275]:


cur.execute("""CREATE TABLE IF NOT EXISTS recipe(
   id INT PRIMARY KEY,
   name VARCHAR, 
   minutes INT, 
   submitted VARCHAR, 
   description TEXT, 
   n_ingredients INT);
""")
conn.commit()


# In[487]:


cur.execute("""DROP TABLE tag""")


# In[488]:


cur.execute("""CREATE TABLE IF NOT EXISTS Tag(
   tag VARCHAR,
   recipe_id INT, 
   primary key (tag, recipe_id)
   );
""")
conn.commit()


# In[247]:


cur.execute("""CREATE TABLE IF NOT EXISTS ingridient(
   ingridient_name VARCHAR,
   recipe_id INT, 
   primary key (ingridient_name, recipe_id)
   );
""")
conn.commit()


# ![image-4.png](attachment:image-4.png)

# 2\. Загрузите данные из файла `recipes_sample.csv` в таблицу `Recipe`. При выполнении задания воспользуйтесь методом `executemany` объекта-курсора.

# In[276]:


import pandas as pd


# In[277]:


df = pd.read_csv('recipes_sample.csv')
df
df['n_ingridients'] = df['n_ingredients']
df = df[['id', 'name', 'minutes', 'submitted', 'description', 'n_ingredients']]
tuples = [tuple(x) for x in df.to_numpy()]


# In[278]:


tuples


# In[279]:


sql = '''
 INSERT INTO recipe(id, name, minutes, submitted, description, n_ingredients) VALUES (?, ?, ?, ?, ?, ?)
'''


cur.executemany(sql, tuples)
conn.commit()


# In[281]:


# df = pd.read_csv('recipes_sample.csv')
# df
# df['n_ingridients'] = df['n_ingredients']
# df = df[['id', 'name', 'minutes', 'submitted', 'description', 'n_ingredients']]
# df.to_sql('recipe', conn, if_exists='append', index=False)


# In[282]:


a = cur.execute("SELECT * FROM recipe").fetchall()


# 3\. Загрузите данные из файла `tags_sample.pickle` в таблицу `Tag`. При выполнении задания воспользуйтесь методом `executemany` объекта-курсора. Для считывания файла с данными воспользуйтесь пакетом `pickle`.

# In[489]:


import pandas as pd
import pickle 
import pandas as pd
unpickled_df = pd.read_pickle("tags_sample.pickle")  
 
with open('tags_sample.pickle', 'rb') as f:
    tags_sample = pickle.load(f)
df_tags = pd.DataFrame(tags_sample)
df_tags['tag'] = df_tags['tag'].astype(str).str[1:-1]


# In[490]:


df_tags


# In[491]:


tuples2 = [tuple(x) for x in df_tags.to_numpy()]
cur.executemany("INSERT INTO tag VALUES(?, ?)", tuples2)


# In[492]:


a = cur.execute("SELECT * FROM tag").fetchall()
a


# 4\. Загрузите данные из файла `reviews_sample.csv` в таблицу `Review`. При выполнении задания воспользуйтесь методом `pd.DataFrame.to_sql`.

# In[286]:


df = pd.read_csv('reviews_sample.csv')
df['id'] = df.iloc[:,0]
df = df[['id', 'user_id', 'recipe_id', 'date', 'rating', 'review']]
df.to_sql('review', conn, if_exists='append', index=False)


# 5\. Загрузите данные из файла `ingredients_sample.csv` в таблицу `Ingredients`. При выполнении задания воспользуйтесь методом `DataFrame.to_sql`.
# 
# Обратите внимание, перед вызовом метода `to_sql` вам требуется привести фрейм к соответствующему таблице в БД виду.

# In[232]:


df = pd.read_csv('ingredients_sample.csv')
df = df.rename(columns = {'recipe':'recipe_id', 'ingredients':'ingridient_name'})


# In[233]:


df.to_sql('ingridient', conn, if_exists='append', index=False)


# ### Получение данных из базы

# 6\. Напишите и выполните запрос на языке SQL, который считает кол-во рецептов, опубликованных в 2010 году и имеющих длину не менее 15 минут. Для выполнения запроса используйте метод `execute` объекта-курсора. Выведите искомое количество на экран.

# In[498]:


# SELECT COUNT(*) FROM имя_таблицы WHERE условие

cur.execute("SELECT COUNT(*) FROM recipe WHERE minutes >= 15 and submitted like '%2010%' ").fetchone()[0]


# 7\. Напишите и выполните запрос на языке SQL, который возращает id рецептов, не имеющих ни одного отзыва отзывов с рейтингом, меньше 4. Для выполнения запроса используйте функцию `pd.read_sql_query`. Выведите полученный результат на экран.

# In[501]:


iris_frame = pd.read_sql_query(
            "SELECT id FROM recipe where id not in (SELECT recipe_id FROM review WHERE rating < 4)", conn)


# In[502]:


iris_frame


# 8\. Создайте `pd.DataFrame`, содержащий данные из таблицы `Tag`. Создайте `pd.DataFrame`, содержащий данные из таблицы `Recipe` (для создания фреймов можно воспользоваться функцией `read_sql_query`). 
# 
# Используя механизмы группировки и объединения, которые предоставляет `pandas`, выведите на экран названия и количество тегов 5 рецептов, которые имеют наибольшее количество тэгов. Измерьте время выполнения работы вашего кода (в замеры включите время, которое тратится на загрузку таблиц).

# In[323]:


recipe_frame = pd.read_sql_query(
            "SELECT * FROM recipe ", conn)


# In[324]:


recipe_frame


# In[493]:


tag_frame = pd.read_sql_query(
            "SELECT * FROM tag ", conn)
tag_frame = tag_frame.rename(columns={"tag": "recipe_id", "recipe_id": "tag"})
tag_frame['len'] = tag_frame.tag.str.split(',').str.len()
tag_frame['len'] = tag_frame.tag.str.split(',').str.len()
tag_frame.sort_values(['len'], ascending=False).head(5)
tag_frame.sort_values(['len'], ascending=False).head(5)


# In[494]:


tag_frame['recipe_id'] = tag_frame.recipe_id.astype('int64')
 
tag_frame.dtypes


# In[495]:


merge_df = tag_frame.merge(recipe_frame, left_on='recipe_id', right_on='id', how = 'inner') 
merge_df = tag_frame.merge(recipe_frame, left_on='recipe_id', right_on='id', how = 'inner') 
merge_df.sort_values(['len'], ascending=False).head(5)[['name', 'len']]
merge_df.sort_values(['len'], ascending=False).head(5)[['name', 'len']]


# In[496]:


get_ipython().run_cell_magic('time', '', 'recipe_frame = pd.read_sql_query(\n            "SELECT * FROM recipe ", conn)\ntag_frame = pd.read_sql_query(\n            "SELECT * FROM tag ", conn)\ntag_frame = tag_frame.rename(columns={"tag": "recipe_id", "recipe_id": "tag"})\ntag_frame[\'len\'] = tag_frame.tag.str.split(\',\').str.len()\ntag_frame[\'recipe_id\'] = tag_frame.recipe_id.astype(\'int64\')\nmerge_df = tag_frame.merge(recipe_frame, left_on=\'recipe_id\', right_on=\'id\', how = \'inner\') \nmerge_df.sort_values([\'len\'], ascending=False).head(5)[[\'name\', \'len\']]')


# 9\. Используя механизмы группировки и объединения, которые предоставляет SQL, выведите на экран названия и количество тегов 5 рецептов, которые имеют наибольшее количество тэгов. При выполнении задания воспользуйтесь методом `execute` объекта-курсора. Измерьте время выполнения работы вашего кода.
# 
# Вся необходимая логика (группировки, объединения, выбор топ-5 строк) должна быть реализована на SQL, а не в виде кода на Python.

# 10\. Запросите у пользователя id рецепта и верните информацию об этом рецепте. Если рецепт отсуствует, выведите соответствующее сообщение. Для подстановки значения id необходимо воспользоваться специальным синтаксисом, которые предоставляет `sqlite` для этих целей.Продемонстрируйте работоспособность вашего решения.

# In[497]:


rec_id = input('Введите id ')
info = cur.execute(f"""SELECT * 
FROM recipe, ingridient, review, tag
WHERE  recipe.id = ingridient.recipe_id
AND ingridient.recipe_id = review.recipe_id
AND recipe.id = tag.tag
AND recipe.id = ?;""", (rec_id, )).fetchall()
if not info:
    print('No inormation')
else:
    print(info)


# In[ ]:





import pandas as pd
import tweepy
import pymysql
import csv
import json
from datetime import date


def citacoes(dataset, data):
    n_lista = []
    cnt = 0

    for categoria in dataset.track_name:
        # contagem de twitts
        cont_twitts = 1
        #
        for tweet in tweepy.Cursor(twitter.search, q=categoria, count=100,
                                   result_type='recent', since=data).items():

            #Salvar os twitts em um arquivo csv
            csvWriter.writerow([tweet.created_at, tweet.text.encode('utf-8')])
            cont_twitts += 1

        # Inserindo a quantidade de twitts encontrados por busca
        if cont_twitts != 0:
            cont_twitts += 1


        n_lista.insert(cnt, cont_twitts)
        cnt += 1
    return n_lista



###################################### Manipulando os Dados ########################################################
#Lendo o arquivo CSV
data = pd.read_csv('AppleStore.csv')

#Dataset com Genero igual a News
dataNews = data.loc[data.prime_genre == 'News']
News = dataNews[dataNews.rating_count_tot == dataNews.rating_count_tot.max()]

#Dataset apenas com as músicas e livros
data_Music = data.loc[data.prime_genre == 'Music']
data_Book = data.loc[data.prime_genre == 'Book']

################################################## Twitter ########################################################

consumer_key = 'eGrW9GyhCtJeA48LsP0GLZzY4'
consumer_secret = 'D7Pl1MrTLlyX9Q58XLdARjDZwYHyas9VSeh6WvaJLaecXXgnWB'
access_token = '1025573849725906944-Hqg0ccKbYFSdJraSRB73vUSBqYiX6u'
access_token_secret = 'vwD8dPazpJtkBInUj4QZrjsfx6LojLsMhBKNfaWWn6cIy'

auth = tweepy.OAuthHandler(consumer_key, consumer_secret)
auth.set_access_token(access_token, access_token_secret)

# conectando com a API
twitter = tweepy.API(auth, wait_on_rate_limit=True, wait_on_rate_limit_notify=True)
data = date.today()

# Open/Create a file to append data
csvFile = open('twitters.csv', 'a')
csvWriter = csv.writer(csvFile)
csvWriter.writerow(['Data_de_Criacao ','Textos'])

#Busca de Citaçoes
data_Music['n_citacoes'] = citacoes(data_Music, data)
data_Book['n_citacoes'] = citacoes(data_Book, data)

#Ordenando os Dados de acordo com o Numero de Citaçoes
data_nB = data_Book.sort_values('n_citacoes', ascending=False)
data_nM = data_Music.sort_values('n_citacoes', ascending=False)

#Criando um Dataset Apenas com os 10 primeiros de Cada Genero: Book e Music
DataN = pd.concat([data_nB[:10], data_nM[:10]])
#Armazenando os Dados em um Arquivo CSV
DataN.to_csv('arquivoCog.csv', columns= ['id', 'track_name', 'n_citacoes',
                                         'size_bytes', 'price', 'prime_genre'], index  = False)

######################## Armazenar os Dados do Arquivo Criado em uma Base de Dados ####################################
#Conexao com o Banco
con = pymysql.connect(host = 'localhost',
                      user = 'root',
                      password = 'sync540n',
                      database = 'cognitivo_db')
cursor = con.cursor()

#Lendo o arquivo CSV
arquivo = csv.DictReader(open("arquivoCog.csv", encoding='utf-8'))

#Inserir os Dados no banco 'cognitivo_db'  na tabela 'citacoes'
for linha in arquivo:
    cursor.execute("insert into citacoes (id, track_name, n_citacoes, size_bytes, price, prime_genre) \
    values (%s, %s, %s, %s, %s, %s)",(linha['id'],linha['track_name'],linha['n_citacoes'],linha['size_bytes'],
    linha['price'],linha['prime_genre']))
    con.commit()


################################### Retornar um Json a partir do CSV ##############################################
arquivoJson = open('ArqCognitivo.json', 'w')
reader = csv.DictReader(open("arquivoCog.csv", encoding='utf-8'))
for linha in reader:
    json.dump(linha, arquivoJson)
    arquivoJson.write('\n')


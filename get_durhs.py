import asyncio
import asyncpg
import pandas as pd
import geopandas as gpd


loop = asyncio.get_event_loop()

async def main(numero_durh):
    conn = await asyncpg.connect('postgresql://adm_geout:ssdgeout@10.207.30.15:5432/geout')
    numerodurh = numero_durh
    data = await conn.fetchrow(f"""
SELECT *
FROM 
   (SELECT *
     FROM durhs_filtradas_completas AS d
     WHERE d.numerodurh = '{numerodurh}' and (d.situacaodurh = 'Validada' OR d.situacaodurh = 'Sujeita a outorga')
    ) AS dunica,  
   

  (SELECT 
     sub.fid, sub.cobacia, sub.cocursodag, sub.dn, sub.q_q95espjan, sub.q_q95espfev,
     sub.q_q95espmar, sub.q_q95espabr, sub.q_q95espmai, sub.q_q95espjun, sub.q_q95espjul,
     sub.q_q95espago, sub.q_q95espset, sub.q_q95espout, sub.q_q95espnov, sub.q_q95espdez,
     sub.q_dq95jan, sub.q_dq95fev, sub.q_dq95mar, sub.q_dq95abr, sub.q_dq95mai, sub.q_dq95jun,
     sub.q_dq95jul, sub.q_dq95ago, sub.q_dq95set, sub.q_dq95out, sub.q_dq95nov, sub.q_dq95dez, sub.q_q95espano,
     ST_Distance(sub.geom, ST_Transform (d.geometry, 3857)) As act_dist
     FROM subtrechos AS sub, otto_minibacias_pol_100k AS mini, durhs_filtradas_completas AS d
     WHERE d.numerodurh = '{numerodurh}' AND
         mini.cobacia = (SELECT mini.cobacia
                           FROM
                           durhs_filtradas_completas AS d,
                           otto_minibacias_pol_100k AS mini
                           WHERE
                          d.numerodurh = '{numerodurh}'
                           AND ST_INTERSECTS(ST_Transform (d.geometry, 3857), mini.wkb_geometry)
                           GROUP BY mini.cobacia
                        )    
           AND ST_INTERSECTS(sub.geom, mini.wkb_geometry)
     ORDER BY act_dist
     LIMIT 1
  ) As sel 
        """)
    await conn.close()
    print(data)
    return data


async def get_minibacia(data):
  cobacia = data['cobacia']
  cocursodag = data['cocursodag']
  conn = await asyncpg.connect('postgresql://adm_geout:ssdgeout@10.207.30.15:5432/geout')
  bacia_select = await conn.fetch(f"""
SELECT o.cobacia, o.cocursodag, o.cotrecho, o.wkb_geometry as geometry 
FROM otto_minibacias_pol_100k AS o
WHERE ((o.cocursodag) LIKE ('{cocursodag}%')) AND ((o.cobacia) >= ('{cobacia}'))
    """)
  await conn.close()
  colnames = [key for key in bacia_select[0].keys()]
  df = pd.DataFrame(bacia_select, columns= colnames)
  gdf = gpd.GeoDataFrame(df)
  gdf['geometry'] = gpd.GeoSeries.from_wkb(gdf['geometry'])
  gdf.iloc[:, 0:3] = gdf.iloc[:, 0:3].astype(str)
  gdf.set_crs(epsg='3857', inplace=True)
  return gdf, df


async def get_valid_durhs(data):
  cobacia = data['cobacia']
  cocursodag = data['cocursodag']
  conn = await asyncpg.connect('postgresql://adm_geout:ssdgeout@10.207.30.15:5432/geout')
  durhs_select = await conn.fetch(f"""
SELECT *
FROM durhs_filtradas_completas AS d  
WHERE ST_within(ST_Transform(d.geometry, 3857), (SELECT ST_UNION(ST_GeomFromEWKB(o.wkb_geometry)) as geom 
                         FROM otto_minibacias_pol_100k AS o 
                         WHERE ((o.cocursodag) LIKE ('{cocursodag}%')) 
                         AND ((o.cobacia) >= ('{cobacia}')))
         )
And (d.situacaodurh = 'Validada' 
OR d.situacaodurh = 'Sujeita a outorga')
    """)
  colnames = [key for key in durhs_select[0].keys()]
  df_durhs_select = pd.DataFrame(durhs_select, columns= colnames)
  gdf_durhs_select = gpd.GeoDataFrame(df_durhs_select)
  gdf_durhs_select['geometry'] = gpd.GeoSeries.from_wkb(gdf_durhs_select['geometry'])
  gdf_durhs_select.iloc[:, 0:3] = gdf_durhs_select.iloc[:, 0:3].astype(str)
  gdf_durhs_select.set_crs(epsg='3857', inplace=True)
  return gdf_durhs_select


# Cálculo das Vazões sazonais com base na cobacia do subtrecho
# UNIDADE SAI EM m³/s

def ConVazoesSazonais(data):
  DQ95ESPMES = [data['q_q95espjan'], data['q_q95espfev'],
             data['q_q95espmar'], data['q_q95espabr'],
             data['q_q95espmai'], data['q_q95espjun'],
             data['q_q95espjul'], data['q_q95espago'],
             data['q_q95espset'], data['q_q95espout'],
             data['q_q95espnov'], data['q_q95espdez']]

  Q95Local = [data['q_dq95jan']*1000, data['q_dq95fev']*1000,
              data['q_dq95mar']*1000, data['q_dq95abr']*1000,
              data['q_dq95mai']*1000, data['q_dq95jun']*1000,
              data['q_dq95jul']*1000, data['q_dq95ago']*1000,
              data['q_dq95set']*1000, data['q_dq95out']*1000,
              data['q_dq95nov']*1000, data['q_dq95dez']*1000]
  Qoutorgavel = [i * 0.5 for i in Q95Local]
  return DQ95ESPMES, Q95Local, Qoutorgavel


# CONSULTA DE INFORMAÇÕES DA DURH ANALISADA
def getinfodurh(data):
  # VAZÃO POR DIA
  Qls = [data['dad_qt_vazaodiajan'],data['dad_qt_vazaodiafev'],
         data['dad_qt_vazaodiamar'],data['dad_qt_vazaodiaabr'],
         data['dad_qt_vazaodiamai'],data['dad_qt_vazaodiajun'],
         data['dad_qt_vazaodiajul'],data['dad_qt_vazaodiaago'],
         data['dad_qt_vazaodiaset'],data['dad_qt_vazaodiaout'],
         data['dad_qt_vazaodianov'],data['dad_qt_vazaodiadez']]
# HORAS POR DIA
  HD = [data['dad_qt_horasdiajan'],data['dad_qt_horasdiafev'],
        data['dad_qt_horasdiamar'],data['dad_qt_horasdiaabr'],
        data['dad_qt_horasdiamai'],data['dad_qt_horasdiajun'],
        data['dad_qt_horasdiajul'],data['dad_qt_horasdiaago'],
        data['dad_qt_horasdiaset'],data['dad_qt_horasdiaout'],
        data['dad_qt_horasdianov'],data['dad_qt_horasdiadez']]
# DIA POR MES
  DM = [data['dad_qt_diasjan'],data['dad_qt_diasfev'],
        data['dad_qt_diasmar'],data['dad_qt_diasabr'],
        data['dad_qt_diasmai'],data['dad_qt_diasjun'],
        data['dad_qt_diasjul'],data['dad_qt_diasago'],
        data['dad_qt_diasset'],data['dad_qt_diasout'],
        data['dad_qt_diasnov'],data['dad_qt_diasdez']]
# HORAS POR MES
  HM = [float(data['dad_qt_horasdiajan'])*float(data['dad_qt_diasjan']),
        float(data['dad_qt_horasdiafev'])*float(data['dad_qt_diasfev']),
        float(data['dad_qt_horasdiamar'])*float(data['dad_qt_diasmar']),
        float(data['dad_qt_horasdiaabr'])*float(data['dad_qt_diasabr']),
        float(data['dad_qt_horasdiamai'])*float(data['dad_qt_diasmai']),
        float(data['dad_qt_horasdiajun'])*float(data['dad_qt_diasjun']),
        float(data['dad_qt_horasdiajul'])*float(data['dad_qt_diasjul']),
        float(data['dad_qt_horasdiaago'])*float(data['dad_qt_diasago']),
        float(data['dad_qt_horasdiaset'])*float(data['dad_qt_diasset']),
        float(data['dad_qt_horasdiaout'])*float(data['dad_qt_diasout']),
        float(data['dad_qt_horasdianov'])*float(data['dad_qt_diasnov']),
        float(data['dad_qt_horasdiadez'])*float(data['dad_qt_diasdez'])]
# M³ POR MÊS
  M3 = [((x*y)*3.6) for x,y in zip(HM,Qls)]
# DIC DE INFORMAÇÕES
  dic_durh = {"Vazão/Dia":Qls,
         "Horas/Mês":list(map(int, HM)),
         "Horas/Dia":list(map(int, HD)),
          "Dia/Mês":list(map(float, DM)),
          "M³/Mês":M3}
  # CRIAR DATAFRAME
  dfinfos = pd.DataFrame(dic_durh,index=['Jan', 'Fev', 'Mar', 'Abr', 'Mai', 'Jun', 'Jul', 'Ago', 'Set', 'Out', 'Nov', 'Dez'])

  return dfinfos


#FUNÇÃO DE VAZAO DAS DURHS VALIDADAS

async def get_valid_durhs(data):
  cobacia = data['cobacia']
  cocursodag = data['cocursodag']
  conn = await asyncpg.connect('postgresql://adm_geout:ssdgeout@10.207.30.15:5432/geout')
  durhs_select = await conn.fetch(f"""
SELECT *
FROM durhs_filtradas_completas AS d  
WHERE ST_within(ST_Transform(d.geometry, 3857), (SELECT ST_UNION(ST_GeomFromEWKB(o.wkb_geometry)) as geom 
                         FROM otto_minibacias_pol_100k AS o 
                         WHERE ((o.cocursodag) LIKE ('{cocursodag}%')) 
                         AND ((o.cobacia) >= ('{cobacia}')))
         )
AND (d.situacaodurh = 'Validada' 
AND d.pontointerferencia = 'Captação Superficial')
    """)
  colnames = [key for key in durhs_select[0].keys()]
  df_durhs_select = pd.DataFrame(durhs_select, columns= colnames)
  gdf_durhs_select = gpd.GeoDataFrame(df_durhs_select)
  gdf_durhs_select['geometry'] = gpd.GeoSeries.from_wkb(gdf_durhs_select['geometry'])
  gdf_durhs_select.iloc[:, 0:3] = gdf_durhs_select.iloc[:, 0:3].astype(str)
  gdf_durhs_select.set_crs(epsg='3857', inplace=True)
  tot_durh_jan = gdf_durhs_select['dad_qt_vazaodiajan'] != 0
  count_durhs_jan = tot_durh_jan.count()
  tot_durh_fev = gdf_durhs_select['dad_qt_vazaodiafev'] != 0
  count_durhs_fev = tot_durh_fev.count()
  tot_durh_mar = gdf_durhs_select['dad_qt_vazaodiamar'] != 0
  count_durhs_mar = tot_durh_mar.count()
  tot_durh_abr = gdf_durhs_select['dad_qt_vazaodiaabr'] != 0
  count_durhs_abr = tot_durh_abr.count()
  tot_durh_mai = gdf_durhs_select['dad_qt_vazaodiamai'] != 0
  count_durhs_mai = tot_durh_mai.count()
  tot_durh_jun = gdf_durhs_select['dad_qt_vazaodiajun'] != 0
  count_durhs_jun = tot_durh_jun.count()
  tot_durh_jul = gdf_durhs_select['dad_qt_vazaodiajul'] != 0
  count_durhs_jul = tot_durh_jul.count()
  tot_durh_ago = gdf_durhs_select['dad_qt_vazaodiaago'] != 0
  count_durhs_ago = tot_durh_ago.count()
  tot_durh_set = gdf_durhs_select['dad_qt_vazaodiaset'] != 0
  count_durhs_set = tot_durh_set.count()
  tot_durh_out = gdf_durhs_select['dad_qt_vazaodiaout'] != 0
  count_durhs_out = tot_durh_out.count()
  tot_durh_nov = gdf_durhs_select['dad_qt_vazaodianov'] != 0
  count_durhs_nov = tot_durh_nov.count()
  tot_durh_dez = gdf_durhs_select['dad_qt_vazaodiadez'] != 0
  count_durhs_dez = tot_durh_dez.count()
  # Produtos finais adquiridos no return
  total_durhs_mont = [count_durhs_jan,count_durhs_fev,count_durhs_mar,count_durhs_abr,
                      count_durhs_mai,count_durhs_jun,count_durhs_jul,count_durhs_ago,
                      count_durhs_set,count_durhs_out,count_durhs_nov,count_durhs_dez]
  vaz_durhs_mont = [sum(gdf_durhs_select.dad_qt_vazaodiajan.fillna(0)), sum(gdf_durhs_select.dad_qt_vazaodiafev.fillna(0)),
                  sum(gdf_durhs_select.dad_qt_vazaodiamar.fillna(0)), sum(gdf_durhs_select.dad_qt_vazaodiaabr.fillna(0)),
                  sum(gdf_durhs_select.dad_qt_vazaodiamai.fillna(0)), sum(gdf_durhs_select.dad_qt_vazaodiajun.fillna(0)),
                  sum(gdf_durhs_select.dad_qt_vazaodiajul.fillna(0)), sum(gdf_durhs_select.dad_qt_vazaodiaago.fillna(0)),
                  sum(gdf_durhs_select.dad_qt_vazaodiaset.fillna(0)), sum(gdf_durhs_select.dad_qt_vazaodiaout.fillna(0)),
                  sum(gdf_durhs_select.dad_qt_vazaodianov.fillna(0)), sum(gdf_durhs_select.dad_qt_vazaodiadez.fillna(0))]
  return total_durhs_mont, vaz_durhs_mont

# CONSULTA DE VAZOES DAS OUTORGAS À MONTANTE


async def get_cnarh40_mont(data):
  cobacia = data['cobacia']
  cocursodag = data['cocursodag']
  conn = await asyncpg.connect('postgresql://adm_geout:ssdgeout@10.207.30.15:5432/geout')
  cnarh_select = await conn.fetch(f"""
SELECT *
FROM cnarh40 AS cn  
WHERE ST_within(ST_Transform(cn.geom, 3857), (SELECT ST_UNION(ST_GeomFromEWKB(o.wkb_geometry)) as geom 
                         FROM otto_minibacias_pol_100k AS o 
                         WHERE ((o.cocursodag) LIKE ('{cocursodag}%')) 
                         AND ((o.cobacia) >= ('{cobacia}')))
         )
AND (cn.int_tin_ds = 'Captação' 
AND cn.int_tch_ds = 'Rio ou Curso D''Água')
    """)
  colnames = [key for key in cnarh_select[0].keys()]
  df_cnarh_select = pd.DataFrame(cnarh_select, columns= colnames)
  gdf_cnarh_select = gpd.GeoDataFrame(df_cnarh_select)
  gdf_cnarh_select['geom'] = gpd.GeoSeries.from_wkb(gdf_cnarh_select['geom'])
  gdf_cnarh_select['geom'].set_crs(epsg='3857', inplace=True)
  gdf_cnarh_select.loc[:,'dad_qt_vazaodiajan':'dad_qt_vazaodiadez'] = gdf_cnarh_select.loc[:,'dad_qt_vazaodiajan':'dad_qt_vazaodiadez'].astype(str).stack().str.replace('.','', regex=True).unstack()
  gdf_cnarh_select.loc[:,'dad_qt_vazaodiajan':'dad_qt_vazaodiadez'] = gdf_cnarh_select.loc[:,'dad_qt_vazaodiajan':'dad_qt_vazaodiadez'].astype(str).stack().str.replace(',','.', regex=True).unstack()
  gdf_cnarh_select.loc[:,'dad_qt_vazaodiajan':'dad_qt_vazaodiadez'] = gdf_cnarh_select.loc[:,'dad_qt_vazaodiajan':'dad_qt_vazaodiadez'].replace("None", "0")
  gdf_cnarh_select.loc[:, 'dad_qt_vazaodiajan':'dad_qt_vazaodiadez'] = gdf_cnarh_select.loc[:, 'dad_qt_vazaodiajan':'dad_qt_vazaodiadez'].astype(float)
  tot_cnarh_jan = gdf_cnarh_select[gdf_cnarh_select['dad_qt_vazaodiajan'] != 0]
  count_cnarh_jan = tot_cnarh_jan[tot_cnarh_jan.columns[0]].count()
  tot_cnarh_fev = gdf_cnarh_select[gdf_cnarh_select['dad_qt_vazaodiafev'] != 0]
  count_cnarh_fev = tot_cnarh_fev[tot_cnarh_fev.columns[0]].count()
  tot_cnarh_mar = gdf_cnarh_select[gdf_cnarh_select['dad_qt_vazaodiamar'] != 0]
  count_cnarh_mar = tot_cnarh_mar[tot_cnarh_mar.columns[0]].count()
  tot_cnarh_abr = gdf_cnarh_select[gdf_cnarh_select['dad_qt_vazaodiaabr'] != 0]
  count_cnarh_abr = tot_cnarh_abr[tot_cnarh_abr.columns[0]].count()
  tot_cnarh_mai = gdf_cnarh_select[gdf_cnarh_select['dad_qt_vazaodiamai'] != 0]
  count_cnarh_mai = tot_cnarh_mai[tot_cnarh_mai.columns[0]].count()
  tot_cnarh_jun = gdf_cnarh_select[gdf_cnarh_select['dad_qt_vazaodiajun'] != 0]
  count_cnarh_jun = tot_cnarh_jun[tot_cnarh_jun.columns[0]].count()
  tot_cnarh_jul = gdf_cnarh_select[gdf_cnarh_select['dad_qt_vazaodiajul'] != 0]
  count_cnarh_jul = tot_cnarh_jul[tot_cnarh_jul.columns[0]].count()
  tot_cnarh_ago = gdf_cnarh_select[gdf_cnarh_select['dad_qt_vazaodiaago'] != 0]
  count_cnarh_ago = tot_cnarh_ago[tot_cnarh_ago.columns[0]].count()
  tot_cnarh_set = gdf_cnarh_select[gdf_cnarh_select['dad_qt_vazaodiaset'] != 0]
  count_cnarh_set = tot_cnarh_set[tot_cnarh_set.columns[0]].count()
  tot_cnarh_out = gdf_cnarh_select[gdf_cnarh_select['dad_qt_vazaodiaout'] != 0]
  count_cnarh_out = tot_cnarh_out[tot_cnarh_out.columns[0]].count()
  tot_cnarh_nov = gdf_cnarh_select[gdf_cnarh_select['dad_qt_vazaodianov'] != 0]
  count_cnarh_nov = tot_cnarh_nov[tot_cnarh_nov.columns[0]].count()
  tot_cnarh_dez = gdf_cnarh_select[gdf_cnarh_select['dad_qt_vazaodiadez'] != 0]
  count_cnarh_dez = tot_cnarh_dez[tot_cnarh_dez.columns[0]].count()
  total_outorgas = [count_cnarh_jan,count_cnarh_fev,count_cnarh_mar,count_cnarh_abr,
                  count_cnarh_mai,count_cnarh_jun,count_cnarh_jul,count_cnarh_ago,
                  count_cnarh_set,count_cnarh_out,count_cnarh_nov,count_cnarh_dez]
  # Soma da DAD_QT_VAZAODIAMES e converter p L/s (*1000)/3600
  vazao_tot_cnarh = [round(sum(gdf_cnarh_select['dad_qt_vazaodiajan']/3.6), 2), round(sum(gdf_cnarh_select['dad_qt_vazaodiafev']/3.6), 2),
               round(sum(gdf_cnarh_select['dad_qt_vazaodiamar']/3.6), 2), round(sum(gdf_cnarh_select['dad_qt_vazaodiaabr']/3.6), 2),
               round(sum(gdf_cnarh_select['dad_qt_vazaodiamai']/3.6), 2), round(sum(gdf_cnarh_select['dad_qt_vazaodiajun']/3.6), 2),
               round(sum(gdf_cnarh_select['dad_qt_vazaodiajul']/3.6), 2), round(sum(gdf_cnarh_select['dad_qt_vazaodiaago']/3.6), 2),
               round(sum(gdf_cnarh_select['dad_qt_vazaodiaset']/3.6), 2), round(sum(gdf_cnarh_select['dad_qt_vazaodiaout']/3.6), 2),
               round(sum(gdf_cnarh_select['dad_qt_vazaodianov']/3.6), 2), round(sum(gdf_cnarh_select['dad_qt_vazaodiadez']/3.6), 2)]
  return total_outorgas,vazao_tot_cnarh


def analise(data):
  total_outorgas, vazao_tot_cnarh = loop.run_until_complete(get_cnarh40_mont(data))
  DQ95ESPMES, Q95Local, Qoutorgavel = ConVazoesSazonais(data)
  total_durhs_mont, vaz_durhs_mont = loop.run_until_complete(get_valid_durhs(data))
  dfinfos = getinfodurh(data)
  dfinfos['Q95 local l/s'] = Q95Local
  dfinfos['Q95 Esp l/s/km²'] = DQ95ESPMES
  dfinfos['Durhs val à mont'] = total_durhs_mont
  dfinfos['vazao total Durhs Montante'] = vaz_durhs_mont
  dfinfos["Qnt de outorgas à mont "] = total_outorgas
  dfinfos["Vazao Total cnarh Montante L/s"] = vazao_tot_cnarh
  dfinfos['Vazão Total à Montante'] = [(x + y) for x, y in zip(vazao_tot_cnarh, vaz_durhs_mont)]
  dfinfos["Comprom individual(%)"] = (dfinfos['Vazão/Dia'] / (dfinfos['Q95 local l/s'] * 0.5)) * 100
  dfinfos["Comprom bacia(%)"] = ((dfinfos['Vazão/Dia'] + dfinfos['Vazão Total à Montante']) / (dfinfos['Q95 local l/s'] * 0.5)) * 100
  dfinfos["Q outorgável"] = Qoutorgavel
  dfinfos["Q disponível"] = [(x - y - z) for x, y, z in zip(Q95Local, Qoutorgavel, (dfinfos['Vazão Total à Montante']))]
  dfinfos.loc[dfinfos['Comprom bacia(%)'] > 100, 'Nivel critico Bacia'] = 'Alto Critico'
  dfinfos.loc[dfinfos['Comprom bacia(%)'] <= 100, 'Nivel critico Bacia'] = 'Moderado Critico'
  dfinfos.loc[dfinfos['Comprom bacia(%)'] <= 80, 'Nivel critico Bacia'] = 'Alerta'
  dfinfos.loc[dfinfos['Comprom bacia(%)'] <= 50, 'Nivel critico Bacia'] = 'Normal'
  return dfinfos

# Função inicial para pegar a localização da Durh
def getlocation(numero_durh,durhs,subtrechos):
  point = durhs.loc[durhs['numerodurh'] == numero_durh]  # AQUI ENTRA NUMERO DA DURH
  location = gpd.sjoin_nearest(point, subtrechos, how='inner')
  if (location.iloc[0]['q_q95espano'] == 0):
    print("Subtrecho em barragem/massa d'agua")  # FUTURO POP-UP DE NOTIFICAÇÃO
  else:
    print("Subtrecho fora de barragem/massa d'agua")
    analise(location)
  return

# função inicial para rodar a localização
def run(numero_durh):
  data = loop.run_until_complete(main(numero_durh))
  if (data['q_q95espano'] == 0):
      print("Subtrecho em barragem/massa d'agua")  # FUTURO POP-UP DE NOTIFICAÇÃO
  else:
      print("Subtrecho fora de barragem/massa d'agua")
      analise(data)
  return

run('DURH033029')
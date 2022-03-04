import asyncio
import asyncpg
import pandas as pd


loop = asyncio.get_event_loop()

async def main():
    conn = await asyncpg.connect('postgresql://adm_geout:ssdgeout@10.207.30.15:5432/geout')
    numerodurh = input("coloca ai:")
    data = await conn.fetchrow(f"""
SELECT *
FROM 
   (SELECT max(d.dataenviodurh) AS dataenvio, max(d.numerodurh) AS durh
     FROM durhs_filtradas_completas AS d
     WHERE d.numerodurh = '{numerodurh}' and (d.situacaodurh = 'Validada' OR d.situacaodurh = 'Sujeita a outorga')
    ) AS dunica,  
   

	(SELECT 
     sub.fid, sub.cobacia, sub.cocursodag, sub.dn, sub.q_q95espjan, sub.q_q95espfev,
     sub.q_q95espmar, sub.q_q95espabr, sub.q_q95espmai, sub.q_q95espjun,
     ST_Distance(sub.geom, ST_Transform (d.geometry, 3857)) As act_dist
     FROM subtrechos1 AS sub, otto_minibacias_pol_100k AS mini, durhs_filtradas_completas AS d
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

data = loop.run_until_complete(main())
gdf, df = loop.run_until_complete(get_minibacia(data))

# ST_AsText(ST_GeomFromWKB(ST_AsEWKB(o.wkb_geometry))) as geometry



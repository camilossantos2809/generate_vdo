import asyncio
import asyncpg
from models import VdoFaixaHora


async def get_unidades(conn):
    stmt = await conn.prepare('''select uni_codigo
    from unidades where uni_ativa='S';''')
    values = await stmt.fetch()
    return [unid["uni_codigo"] for unid in values]


async def main():
    conn = await asyncpg.connect(
        user='postgres', password='rp1064',
        database='wrpdv_df', host='127.0.0.1'
    )
    unids = await get_unidades(conn)

    faixa_hora = VdoFaixaHora(conn, unids)

    await asyncio.gather(faixa_hora.run())
    await conn.close()

asyncio.run(main())

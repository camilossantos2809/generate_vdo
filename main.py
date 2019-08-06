import asyncio
import asyncpg
from typing import List
from models import VdoFaixaHora, VdoFinalizadora


async def get_unidades(conn):
    stmt = await conn.prepare('''select uni_codigo
    from unidades where uni_ativa='S';''')
    values = await stmt.fetch()
    return [unid["uni_codigo"] for unid in values]


async def get_finalizadoras(conn) -> List[str]:
    stmt = await conn.prepare('''
        select fin_codigo
        from finalizadoras
        group by fin_codigo;''')
    values = await stmt.fetch()
    return [fin["fin_codigo"] for fin in values]


async def get_pvds(conn) -> List[str]:
    stmt = await conn.prepare('''
        select est_codigo
        from estac
        group by est_codigo;''')
    values = await stmt.fetch()
    return [pdv["est_codigo"] for pdv in values]


async def main():
    conn = await asyncpg.connect(
        user='postgres', password='rp1064',
        database='wrpdv_df', host='127.0.0.1'
    )
    unids = await get_unidades(conn)
    fin = await get_finalizadoras(conn)
    pdvs = await get_pvds(conn)

    faixa_hora = VdoFaixaHora(conn, unids)
    finalizadora = VdoFinalizadora(conn, unids, pdvs, fin)

    await faixa_hora.run()
    await finalizadora.run()

    await conn.close()

if __name__ == "__main__":
    asyncio.run(main())

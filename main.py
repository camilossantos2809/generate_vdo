import asyncio
from typing import List

import asyncpg

from config import Config
from models import (MetasProd, VdoDepartamento, VdoFaixaHora, VdoFinalizadora,
                    VdoFormaVenda, VdoOperadores)


async def create_sequence(conn):
    await conn.execute("create sequence if not exists generate_vdo start 1")


async def get_unidades(conn) -> List[str]:
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


async def get_departamentos(conn) -> List[str]:
    stmt = await conn.prepare('''
        select lpad(dpt_codigo,4,'0') as dpt_codigo
        from departamentos;''')
    values = await stmt.fetch()
    return [dpto["dpt_codigo"] for dpto in values]


async def get_operadores(conn) -> List[str]:
    stmt = await conn.prepare('''
        select usu_codigo
        from usuario
        where usu_grupo=2
            and usu_ativo='S';
        ''')
    values = await stmt.fetch()
    return [user["usu_codigo"] for user in values]


async def main():
    config = Config()
    conn = await asyncpg.connect(**config.data)

    await create_sequence(conn)
    unids = await get_unidades(conn)
    fin = await get_finalizadoras(conn)
    pdvs = await get_pvds(conn)
    dptos = await get_departamentos(conn)
    operadores = await get_operadores(conn)

    metas = MetasProd(conn, unids, dptos)
    faixa_hora = VdoFaixaHora(conn, unids)
    finalizadora = VdoFinalizadora(conn, unids, pdvs, fin)
    departamento = VdoDepartamento(conn, unids, dptos)
    forma_venda = VdoFormaVenda(conn, unids)
    operador = VdoOperadores(conn, unids, operadores)

    await metas.run()
    await forma_venda.run()
    await faixa_hora.run()
    await finalizadora.run()
    await departamento.run()
    await operador.run()

    await conn.close()

if __name__ == "__main__":
    asyncio.run(main())

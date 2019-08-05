import asyncio
import decimal
import random
import asyncpg


def generate_decimal(div=1000) -> decimal.Decimal:
    return decimal.Decimal(random.randrange(10000))/div


class VdoFaixaHora:
    def __init__(self, conn, unids: list):
        self.conn = conn
        self.unids = unids

    async def _exists(self) -> bool:
        stmt = await self.conn.prepare('''select exists(
            select * from vdofaixahora
            where vfh_data=current_date
                and vfh_hora=extract(hour from current_timestamp)
            )''')
        value = await stmt.fetchrow()
        return value['exists']

    async def _insert(self):
        for unid in self.unids:
            await self.conn.execute('''
                INSERT INTO vdofaixahora (
                    vfh_codseq, vfh_unidade, vfh_data, vfh_hora, 
                    vfh_vendabruta, vfh_cancelamentos, vfh_acrescimos, 
                    vfh_descontos, vfh_qtditens, vfh_qtdcupons)
                VALUES(nextval('generate_vdo'), $7, current_date,
                    extract(hour from current_timestamp), $1, $2, $3, $4, $5, $6)''',
                                    generate_decimal(
                                        100), generate_decimal(), generate_decimal(),
                                    generate_decimal(), generate_decimal(), generate_decimal(),
                                    unid)

    async def _update(self):
        await self.conn.execute('''UPDATE public.vdofaixahora
        SET vfh_vendabruta=vfh_vendabruta+$1, 
            vfh_cancelamentos=vfh_cancelamentos+$2,
            vfh_acrescimos=vfh_acrescimos+$3,
            vfh_descontos=vfh_descontos+$4,
            vfh_qtditens=vfh_qtditens+$5,
            vfh_qtdcupons=vfh_qtdcupons+$6
        where vfh_hora=extract(hour from current_timestamp)
            and vfh_data=current_date
            and vfh_unidade=$7;
        ''', generate_decimal(100), generate_decimal(), generate_decimal(),
            generate_decimal(), generate_decimal(), generate_decimal(),
            random.choice(self.unids))

    async def run(self):
        exist = await self._exists()
        if exist:
            print("update")
            await self._update()
        else:
            print("insert")
            await self._insert()


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

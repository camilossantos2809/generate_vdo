import abc
import random
from datetime import datetime
from typing import List


from utils import generate_decimal

Unids = List[str]
Pdvs = List[str]
Final = List[str]


class Vdo(abc.ABC):
    @abc.abstractmethod
    async def _exists(self) -> bool:
        pass

    @abc.abstractmethod
    async def _update(self):
        pass

    @abc.abstractmethod
    async def _insert(self):
        pass

    async def run(self):
        exist = await self._exists()
        if exist:
            print("update", self.__class__.__name__, datetime.now())
            await self._update()
        else:
            print("insert", self.__class__.__name__, datetime.now())
            await self._insert()


class VdoFaixaHora(Vdo):
    def __init__(self, conn, unids: Unids):
        self.conn = conn
        self.unids: Unids = unids

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


class VdoFinalizadora(Vdo):
    def __init__(self, conn, unids: Unids, pdvs: Pdvs, fin: Final):
        self.conn = conn
        self.unids: Unids = unids
        self.pdvs: Pdvs = pdvs
        self.fin: Final = fin

    async def _exists(self) -> bool:
        stmt = await self.conn.prepare('''select exists(
            select * from vdofinalizadoras
            where vfi_data=current_date
        )''')
        value = await stmt.fetchrow()
        return value['exists']

    async def _insert(self):
        await self.conn.execute('''
            INSERT INTO public.vdofinalizadoras (vfi_codseq,
            vfi_unidade,vfi_pdv,vfi_data,vfi_finalizadora,
            vfi_descricao,vfi_vendabruta,vfi_cancelamentos,
            vfi_acrescimos,vfi_descontos,vfi_qtditens,vfi_qtdcupons)
            with fin as (
                select fin_codigo,initcap(fin_descricao) as fin_descricao
                from finalizadoras
                group by fin_codigo,initcap(fin_descricao)
            )
            select
                nextval('generate_vdo'),uni_codigo,est_codigo,current_date,
                fin_codigo,fin_descricao,$1,$2,$3,$4,$5,$6
            from unidades
                inner join estac on (uni_codigo=est_unidade),
                fin
            where uni_ativa='S';''', generate_decimal(100), generate_decimal(),
                                generate_decimal(), generate_decimal(),
                                generate_decimal(), generate_decimal())

    async def _update(self):
        unid = random.choice(self.unids)
        pdv = random.choice(self.pdvs)
        fin = random.choice(self.fin)
        await self.conn.execute('''UPDATE vdofinalizadoras
        SET vfi_vendabruta=vfi_vendabruta+$1,
            vfi_cancelamentos=vfi_cancelamentos+$2,
            vfi_acrescimos=vfi_acrescimos+$3,
            vfi_descontos=vfi_descontos+$4,
            vfi_qtditens=vfi_qtditens+$5,
            vfi_qtdcupons=vfi_qtdcupons+$6
        where vfi_data=current_date
            and vfi_unidade=$7
            and vfi_pdv=$8
            and vfi_finalizadora=$9;
        ''', generate_decimal(100), generate_decimal(), generate_decimal(),
            generate_decimal(), generate_decimal(), generate_decimal(),
            unid, pdv, str(fin))

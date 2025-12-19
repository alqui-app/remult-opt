// src/db/mssql-safe-knex-provider.ts
import type { Knex } from 'knex';
import knexFactory from 'knex';
import { Filter } from 'remult';
import { KnexDataProvider } from 'remult/remult-knex';

function toDbValue(field: any, val: any) {
  const v = field?.valueConverter?.toDb ? field.valueConverter.toDb(val) : val;
  return Array.isArray(v) ? JSON.stringify(v) : v;
}

export class SafeKnexDataProvider extends KnexDataProvider {
  constructor(public knex: Knex) {
    super(knex);
  }

  // Envuelve el entity data provider para MSSQL
  getEntityDataProvider(entity: any): any {
    const edp = super.getEntityDataProvider(entity) as any;
    if ((this.knex as any).client?.config?.client !== 'mssql') return edp;

    const knex = this.knex;
    const origInsert = edp.insert.bind(edp);

    edp.insert = async (data: any) => {
      try {
        // Intento normal (otros casos de error no relacionados siguen saliendo acá)
        return await origInsert(data);
      } catch (err: any) {
        const msg = [
          err?.message,
          err?.original?.message,
          err?.originalError?.message,
          String(err),
        ]
          .filter(Boolean)
          .join(' | ');

        const looksLikeTriggerOutputError =
          /\bOUTPUT\s+INSERTED\b/i.test(msg) || // "output inserted"
          /\bOUTPUT\s+INTO\b/i.test(msg) || // "output into"
          /\bOUTPUT\b[\s\S]*?\bwithout\s+INTO\s+clause\b/i.test(msg) || // "OUTPUT clause without INTO clause"
          /cannot\s+use\s+the\s+OUTPUT\s+clause/i.test(msg) ||
          /cannot\s+have any enabled triggers.*OUTPUT\s+clause/i.test(msg) ||
          /WITH\s+an\s+INSTEAD\s+OF\s+trigger/i.test(msg);

        if (!looksLikeTriggerOutputError) throw err;

        // Fallback para tablas con trigger: INSERT sin OUTPUT y SELECT SCOPE_IDENTITY()
        const e = await edp.init();

        const insertObject: any = {};
        for (const f of edp.entity.fields) {
          // Evitamos server expressions / readOnly
          if (f.isServerExpression) continue;
          if (f.options?.dbReadOnly) continue;
          if (data[f.key] === undefined) continue;
          insertObject[await e.$dbNameOf(f)] = toDbValue(f, data[f.key]);
        }

        const builder = edp.getEntityFrom(e).insert(insertObject);
        const compiled = builder.toSQL();

        const rawRes: any = await knex.raw(
          compiled.sql +
            '; select cast(SCOPE_IDENTITY() as numeric(38,0)) as [id]',
          compiled.bindings || []
        );

        const row =
          rawRes?.recordset?.[0] ??
          (Array.isArray(rawRes) && Array.isArray(rawRes[0])
            ? rawRes[0][0]
            : (rawRes?.rows?.[0] ?? rawRes?.[0] ?? {}));
        const newId = row?.id ?? row?.ID ?? row?.Id;
        if (newId === undefined || newId === null) {
          throw new Error(
            'Insert fallback con SCOPE_IDENTITY() no devolvió id (¿la PK no es IDENTITY?).'
          );
        }

        const r = await edp.find({
          where: new Filter((x: any) =>
            x.isEqualTo(edp.entity.idMetadata.field, newId)
          ),
        });
        return r[0];
      }
    };

    return edp;
  }
}

// Factory equivalente a createKnexDataProvider, pero retornando el SafeKnexDataProvider
export async function createMssqlSafeKnexDataProvider(config: Knex.Config) {
  const k = knexFactory(config);
  return new SafeKnexDataProvider(k);
}

import {
  DataProvider,
  EntityDataProvider,
  EntityMetadata,
  Filter,
  Sort,
  SqlDatabase,
} from 'remult';
import { SafeKnexDataProvider } from './mssql-safe-knex-provider';
import type { Knex } from 'knex';

// Símbolo interno de Remult para acceder a la información de relaciones
const fieldRelationInfo = Symbol.for('fieldRelationInfo');

/**
 * OptimizedDataProvider que extiende SafeKnexDataProvider para agregar JOINs automáticos
 * para relaciones con defaultIncluded: true
 */
export class OptimizedDataProvider implements DataProvider {
  private entityProviders = new Map<EntityMetadata, EntityDataProvider>();

  constructor(private baseProvider: SafeKnexDataProvider) {
    // console.log('🚀 OptimizedDataProvider: Inicializado con soporte para JOINs automáticos');
  }

  getEntityDataProvider(entity: EntityMetadata): EntityDataProvider {
    let result = this.entityProviders.get(entity);
    if (!result) {
      result = new OptimizedEntityDataProvider(entity, this.baseProvider);
      this.entityProviders.set(entity, result);
    }
    return result;
  }

  async transaction(
    action: (dataProvider: DataProvider) => Promise<any>
  ): Promise<any> {
    if ('transaction' in this.baseProvider) {
      return (this.baseProvider as any).transaction(action);
    }
    return action(this);
  }

  async ensureSchema(entities: EntityMetadata[]): Promise<void> {
    if ('ensureSchema' in this.baseProvider) {
      return (this.baseProvider as any).ensureSchema(entities);
    }
  }
}

/**
 * OptimizedEntityDataProvider que intercepta find() para agregar JOINs
 */
class OptimizedEntityDataProvider implements EntityDataProvider {
  private baseEntityProvider: EntityDataProvider;
  private knex: Knex;

  constructor(
    private entity: EntityMetadata,
    private baseProvider: SafeKnexDataProvider
  ) {
    this.baseEntityProvider = baseProvider.getEntityDataProvider(entity);
    this.knex = (this.baseProvider as any).knex as Knex;
  }

  /**
   * Dialect-aware identifier wrapper (alias/columns/tables). Falls back to [] for MSSQL-style wrapping.
   */
  private wrapIdentifier(id: string): string {
    if (!id) return id;
    if (id.startsWith('[') || id.startsWith('"')) return id;
    // No wrapping for raw fragments/subqueries
    if (id.includes('(') || id.includes(' ') || id.includes('.[')) {
      return id;
    }

    const dialect = (this.knex as any)?.client?.config?.client;
    if (dialect === 'mssql') {
      return `[${id}]`;
    }
    if (dialect === 'pg' || dialect === 'postgres' || dialect === 'postgresql') {
      return `"${id.replace(/"/g, '""')}"`;
    }

    const wrap = this.knex?.client?.wrapIdentifier;
    if (wrap) {
      return wrap.call(this.knex.client, id);
    }
    // Default ANSI quoting
    return `"${id.replace(/"/g, '""')}"`;
  }

  private wrapTableName(name: string): string {
    if (!name) return name;
    if (name.includes('(')) return name;
    if (name.includes('.')) {
      return name
        .split('.')
        .map((part) => this.wrapIdentifier(part))
        .join('.');
    }
    return this.wrapIdentifier(name);
  }

  private col(alias: string, column: string): string {
    return `${this.wrapIdentifier(alias)}.${this.wrapIdentifier(column)}`;
  }

  private alias(name: string): string {
    return this.wrapIdentifier(name);
  }

  private normalizeSql(sql: string): string {
    return sql
      .replace(/\[([^\]]+)\]/g, (_match, id) => this.wrapIdentifier(id))
      .replace(/"main"\."main"\./g, '"main".')
      .replace(/\[main\]\.\[main\]\./g, '[main].');
  }

  private getRelationDetails(
    entity: EntityMetadata,
    relationKey: string
  ): {
    relationField?: any;
    relationInfo?: any;
    relatedEntity?: EntityMetadata;
    fkField?: any;
  } {
    const relationField = entity.fields.toArray().find((f) => f.key === relationKey);
    const relationInfo = relationField ? (relationField as any)[fieldRelationInfo] : undefined;
    const relatedEntity = relationInfo?.toRepo?.metadata;

    const fkFromInfo =
      (relationInfo as any)?.fieldsMetadata?.[0] ||
      (relationInfo as any)?.fieldMetadata;

    const fieldOptionFkKey =
      (relationField?.options as any)?.field ||
      (relationField?.options as any)?.fieldKey;

    const fkFieldByOption = fieldOptionFkKey
      ? entity.fields.toArray().find((f) => f.key === fieldOptionFkKey)
      : undefined;

    const fkFieldByConvention = entity.fields
      .toArray()
      .find((f) => f.key === `${relationKey}Id`);

    const fkField = fkFromInfo || fkFieldByOption || fkFieldByConvention;

    return { relationField, relationInfo, relatedEntity, fkField };
  }

  /**
   * Método auxiliar para convertir @JOIN: a SUBQUERIES cuando no hay JOINs activos
   * - Campos con @JOIN:relation.field → se convierten a subqueries SQL
   * - Campos con referencias a TableName. → se corrigen a [TableName].
   * Retorna un Map con los valores originales para restaurar
   */
  private async cleanJoinReferences(): Promise<Map<string, any>> {
    const originalValues = new Map<string, any>();
    const tableName = this.entity.dbName || this.entity.key;
    const entityKey = this.entity.key;
    const mainAlias = tableName;

    for (const field of this.entity.fields.toArray()) {
      const fieldOptions = (field as any).options;
      if (fieldOptions?.sqlExpression) {
        let sqlExpr =
          typeof fieldOptions.sqlExpression === 'function'
            ? fieldOptions.sqlExpression(this.entity, { useJoins: false })
            : fieldOptions.sqlExpression;

        // Esperar si es una Promise
        if (sqlExpr instanceof Promise) {
          sqlExpr = await sqlExpr;
        }

        if (typeof sqlExpr === 'string') {
          // Si contiene @JOIN: → Convertir a SUBQUERY
          if (sqlExpr.includes('@JOIN:')) {
            originalValues.set(field.key, {
              sqlExpression: fieldOptions.sqlExpression,
            });

            // Convertir @JOIN:relation.field a subquery
            fieldOptions.sqlExpression = () => {
              return this.convertJoinToSubquery(sqlExpr, field.key, mainAlias);
            };
          }
          // Si contiene un alias join_<path>.<campo> (ej: join_ficha.Direccion), convertir a subquery
          else if (/join_[\w_]+\./i.test(sqlExpr)) {
            originalValues.set(field.key, {
              sqlExpression: fieldOptions.sqlExpression,
            });

            fieldOptions.sqlExpression = () => {
              return this.convertJoinAliasReferencesToSubquery(
                sqlExpr,
                mainAlias
              );
            };
          }
          // Si contiene referencias sin alias → corregirlas
          else if (
            sqlExpr.includes(`${tableName}.`) ||
            sqlExpr.includes(`${entityKey}.`)
          ) {
            originalValues.set(field.key, {
              sqlExpression: fieldOptions.sqlExpression,
            });

            const originalSqlExpr = fieldOptions.sqlExpression;
            fieldOptions.sqlExpression = async (entityMetadata: any) => {
              let sql =
                typeof originalSqlExpr === 'function'
                  ? originalSqlExpr(entityMetadata)
                  : originalSqlExpr;

              if (sql instanceof Promise) {
                sql = await sql;
              }

              if (typeof sql === 'string') {
                // Reemplazar TableName. con [TableName].
                sql = sql.replace(
                  new RegExp(`\\b${tableName}\\.`, 'g'),
                  `${this.wrapIdentifier(tableName)}.`
                );
                sql = sql.replace(
                  new RegExp(`\\b${entityKey}\\.`, 'g'),
                  `${this.wrapIdentifier(entityKey)}.`
                );

                sql = this.normalizeSql(sql);
              }

              return sql;
            };
          }
        }
      }
    }

    return originalValues;
  }

  /**
   * Convierte una expresión @JOIN:relation.field o @JOIN:path.to.relation.field a un subquery SQL
   * Soporta paths anidados: @JOIN:reserva.fichaTasador.tasador.Login
   */
  private convertJoinToSubquery(
    sqlExpr: string,
    fieldKey: string,
    tableAlias?: string
  ): string {
    const tableName = this.entity.dbName || this.entity.key;
    const mainTableAlias = tableAlias || tableName;

    // Buscar todas las ocurrencias de @JOIN:path.to.field (soporta paths anidados)
    // Captura todo el path hasta el último segmento que es el campo
    return sqlExpr.replace(/@JOIN:([\w.]+)/g, (match, fullPath) => {
      // Separar el path en partes: ['reserva', 'fichaTasador', 'tasador', 'Login']
      const parts = fullPath.split('.');
      if (parts.length < 2) {
        return 'NULL'; // Necesitamos al menos relation.field
      }

      // El último elemento es el campo, el resto es el path de relaciones
      const fieldName = parts[parts.length - 1];
      const relationPath = parts.slice(0, -1); // ['reserva', 'fichaTasador', 'tasador']

      // Para compatibilidad, si solo hay un nivel, usar la lógica simple
      if (relationPath.length === 1) {
        const relationKey = relationPath[0];
        return this.simpleJoinToSubquery(
          relationKey,
          fieldName,
          mainTableAlias
        );
      }

      // Para paths anidados, generar subquery con múltiples JOINs
      return this.nestedJoinToSubquery(relationPath, fieldName, mainTableAlias);
    });
  }

  /**
   * Genera un subquery simple para un solo nivel de relación
   */
  private simpleJoinToSubquery(
    relationKey: string,
    fieldName: string,
    mainTableAlias: string
  ): string {
    const relationDetails = this.getRelationDetails(this.entity, relationKey);
    const fkField =
      relationDetails.fkField ||
      this.entity.fields.toArray().find((f) => f.key === `${relationKey}Id`);

    if (!fkField) {
      return 'NULL';
    }

    // Buscar la relación en la metadata
    const fieldOptions = (fkField as any).options;
    const relationInfo =
      relationDetails.relationField && (relationDetails.relationField as any)[fieldRelationInfo]
        ? (relationDetails.relationField as any)[fieldRelationInfo]
        : (fkField as any)[fieldRelationInfo];

    if (!relationInfo) {
      return 'NULL';
    }

    const relatedEntity =
      relationDetails.relatedEntity || relationInfo.toRepo?.metadata;
    if (!relatedEntity) {
      return 'NULL';
    }

    const relatedTableName = relatedEntity.dbName || relatedEntity.key;
    const relatedIdField = relatedEntity.idMetadata?.fields?.[0];
    const relatedIdDbName = relatedIdField
      ? (relatedIdField.options as any)?.dbName || relatedIdField.key
      : 'id';

    // Buscar el campo en la entidad relacionada
    const relatedField = relatedEntity.fields.toArray().find((f: any) => {
      const dbName = (f.options as any)?.dbName || f.key;
      return f.key === fieldName || dbName === fieldName;
    });

    if (!relatedField) {
      return 'NULL';
    }

    const relatedFieldDbName =
      (relatedField.options as any)?.dbName || relatedField.key;
    const fkFieldDbName = fieldOptions?.dbName || fkField.key;

    // Si la entidad relacionada tiene sqlExpression (es una subquery), usarla
    const relatedEntityOptions = (relatedEntity as any).options;
    if (relatedEntityOptions?.sqlExpression) {
      let relatedTableExpression =
        typeof relatedEntityOptions.sqlExpression === 'function'
          ? relatedEntityOptions.sqlExpression(relatedEntity)
          : relatedEntityOptions.sqlExpression;

      // Limpiar alias si tiene (con o sin "as", y también después de paréntesis)
      relatedTableExpression = relatedTableExpression
        .replace(/\s+as\s+\w+\s*$/i, '')
        .replace(/\)\s+([a-zA-Z_][a-zA-Z0-9_]*)\s*$/i, ')')
        .trim();

      return this.normalizeSql(
        `(SELECT ${this.wrapIdentifier(
          relatedFieldDbName
        )} FROM ${relatedTableExpression} WHERE ${this.wrapIdentifier(
          relatedIdDbName
        )} = ${this.col(mainTableAlias, fkFieldDbName)})`
      );
    }

    // Subquery normal
    return this.normalizeSql(
      `(SELECT ${this.wrapIdentifier(
        relatedFieldDbName
      )} FROM ${this.wrapTableName(
        relatedTableName
      )} WHERE ${this.wrapIdentifier(relatedIdDbName)} = ${this.col(
        mainTableAlias,
        fkFieldDbName
      )})`
    );
  }

  /**
   * Convierte referencias con alias join_<path>.<campo> a subqueries cuando no hay JOINs activos.
   * Ej: join_ficha.[Direccion] -> subquery usando el path ficha.Direccion
   */
  private convertJoinAliasReferencesToSubquery(
    sqlExpr: string,
    tableAlias: string
  ): string {
    return sqlExpr.replace(
      /join_([\w_]+)\.\[?([\w]+)\]?/gi,
      (_match, rawPath, fieldName) => {
        const relationPath = (rawPath as string).split('_').join('.');
        const joinPattern = `@JOIN:${relationPath}.${fieldName}`;
        return this.convertJoinToSubquery(joinPattern, fieldName, tableAlias);
      }
    );
  }

  /**
   * Genera un subquery con JOINs anidados para paths multi-nivel
   * Ejemplo: reserva.fichaTasador.tasador -> Login
   * (SELECT t2.Login FROM reserva t0
   *  INNER JOIN FichaTasador t1 ON t1.FichaID = t0.FichaID
   *  INNER JOIN Usuario t2 ON t2.UsuarioID = t1.TasadorID
   *  WHERE t0.ReservaID = CierreVenta.ReservaID)
   */
  private nestedJoinToSubquery(
    relationPath: string[],
    fieldName: string,
    mainTableAlias: string
  ): string {
    const tableName = this.entity.dbName || this.entity.key;

    // Navegar por el path para construir los JOINs
    let currentEntity = this.entity;
    const joins: string[] = [];

    for (let i = 0; i < relationPath.length; i++) {
      const relationKey = relationPath[i];
      const fkFieldKey = `${relationKey}Id`;

      // Buscar FK y relación en la entidad actual
      const relationDetails = this.getRelationDetails(currentEntity, relationKey);
      const fkField =
        relationDetails.fkField ||
        currentEntity.fields.toArray().find((f) => f.key === fkFieldKey);

      const relationField =
        relationDetails.relationField ||
        currentEntity.fields.toArray().find((f) => f.key === relationKey);

      if (!fkField || !relationField) {
        return 'NULL';
      }

      const relationInfo =
        relationDetails.relationInfo || (relationField as any)[fieldRelationInfo];
      if (!relationInfo || !relationInfo.toRepo) {
        return 'NULL';
      }

      const nextEntity = relationDetails.relatedEntity || relationInfo.toRepo.metadata;

      // Determinar tabla/subquery - verificar si tiene sqlExpression
      let nextTableExpression: string;
      const nextEntityOptions = (nextEntity as any).options;

      if (nextEntityOptions?.sqlExpression) {
        // Usar sqlExpression (puede ser vista, función, subquery)
        let sqlExpressionValue =
          typeof nextEntityOptions.sqlExpression === 'function'
            ? nextEntityOptions.sqlExpression(nextEntity)
            : nextEntityOptions.sqlExpression;

        // Si es Promise, no podemos manejarlo en método síncrono - usar fallback
        // En la práctica, esto no debería pasar porque los sqlExpression síncronos son comunes
        if (sqlExpressionValue instanceof Promise) {
          // Fallback al nombre de tabla normal
          nextTableExpression = this.wrapTableName(
            nextEntity.dbName || nextEntity.key
          );
        } else {
          // Limpiar alias si tiene (ej: "SELECT ... FROM ... as alias" o "(SELECT ...) alias" → quitar alias)
          // Primero eliminar alias con "as", luego cualquier alias después de un paréntesis de cierre
          nextTableExpression = (sqlExpressionValue as string)
            .replace(/\s+as\s+\w+\s*$/i, '')
            .replace(/\)\s+([a-zA-Z_][a-zA-Z0-9_]*)\s*$/i, ')')
            .trim();

          // Si es un SELECT subquery, asegurar que tenga paréntesis
          if (
            nextTableExpression.trim().toUpperCase().startsWith('SELECT') &&
            !nextTableExpression.trim().startsWith('(')
          ) {
            nextTableExpression = `(${nextTableExpression})`;
          }
        }
      } else {
        // Tabla normal - usar dbName o key
        nextTableExpression = this.wrapTableName(
          nextEntity.dbName || nextEntity.key
        );
      }

      const nextIdField = nextEntity.idMetadata?.fields?.[0];
      const nextIdDbName = nextIdField
        ? (nextIdField.options as any)?.dbName || nextIdField.key
        : 'id';
      const fkDbName = (fkField.options as any)?.dbName || fkField.key;

      const tableAlias = `t${i}`;
      const wrappedAlias = this.alias(tableAlias);

      if (i === 0) {
        // Primera tabla en el FROM
        joins.push(`${nextTableExpression} ${wrappedAlias}`);
      } else {
        // Tablas subsiguientes como INNER JOIN
        const prevAlias = `t${i - 1}`;
        joins.push(
          `INNER JOIN ${nextTableExpression} ${wrappedAlias} ON ${this.col(
            tableAlias,
            nextIdDbName
          )} = ${this.col(prevAlias, fkDbName)}`
        );
      }

      currentEntity = nextEntity;
    }

    // Buscar el campo final en la última entidad
    const lastEntity = currentEntity;
    const targetField = lastEntity.fields.toArray().find((f: any) => {
      const dbName = (f.options as any)?.dbName || f.key;
      return f.key === fieldName || dbName === fieldName;
    });

    if (!targetField) {
      return 'NULL';
    }

    const targetFieldDbName =
      (targetField.options as any)?.dbName || targetField.key;
    const lastTableAlias = `t${relationPath.length - 1}`;
    const firstTableAlias = 't0';

    // Obtener el FK de la primera relación desde la entidad principal
    const firstRelationKey = relationPath[0];
    const firstFkField = this.entity.fields
      .toArray()
      .find((f) => f.key === `${firstRelationKey}Id`);

    if (!firstFkField) {
      return 'NULL';
    }

    const firstFkDbName =
      (firstFkField.options as any)?.dbName || firstFkField.key;

    // Obtener el ID de la primera tabla relacionada
    const firstRelationField = this.entity.fields
      .toArray()
      .find((f) => f.key === firstRelationKey);

    if (!firstRelationField) {
      return 'NULL';
    }

    const firstRelationInfo = (firstRelationField as any)[fieldRelationInfo];
    if (!firstRelationInfo || !firstRelationInfo.toRepo) {
      return 'NULL';
    }

    const firstRelatedEntity = firstRelationInfo.toRepo.metadata;
    const firstIdField = firstRelatedEntity.idMetadata?.fields?.[0];
    const firstIdDbName = firstIdField
      ? (firstIdField.options as any)?.dbName || firstIdField.key
      : 'id';

    // Construir el subquery completo
    return this.normalizeSql(
      `(SELECT ${this.col(lastTableAlias, targetFieldDbName)} FROM ${joins.join(
        ' '
      )} WHERE ${this.col(firstTableAlias, firstIdDbName)} = ${this.col(
        mainTableAlias,
        firstFkDbName
      )})`
    );
  }

  /**
   * Convierte una expresión @JOIN:relation.field a un subquery SQL
   * desde el contexto de un JOIN ya existente (para JOINs anidados)
   */
  private convertJoinToSubqueryFromJoinContext(
    sqlExpr: string,
    fieldKey: string,
    joinAlias: string,
    joinedEntity: EntityMetadata
  ): string {
    // Si es @JOIN:relation.field, convertir a subquery desde el contexto del JOIN
    return sqlExpr.replace(
      /@JOIN:(\w+)\.(\w+)/g,
      (match, relationKey, fieldName) => {
        // Buscar el FK en la entidad joined (ej: tasadorId en FichaTasador)
        const fkField = joinedEntity.fields
          .toArray()
          .find((f) => f.key === `${relationKey}Id`);

        if (!fkField) {
          return 'NULL';
        }

        const fkFieldDbName = (fkField.options as any)?.dbName || fkField.key;

        // Buscar el campo de RELACIÓN (no el FK) para obtener relationInfo
        // En Remult, el relationInfo está en el campo de relación (ej: 'tasador'), no en el FK (ej: 'tasadorId')
        const relationField = joinedEntity.fields
          .toArray()
          .find((f) => f.key === relationKey);

        if (!relationField) {
          return 'NULL';
        }

        // Obtener metadata de la entidad target (ej: Usuario)
        const relationInfo = (relationField as any)[fieldRelationInfo];

        if (!relationInfo?.toRepo?.metadata) {
          return 'NULL';
        }

        const targetEntity = relationInfo.toRepo.metadata;
        const targetTableName = targetEntity.dbName || targetEntity.key;
        const targetIdField = targetEntity.idMetadata?.fields?.[0];
        const targetIdDbName =
          (targetIdField?.options as any)?.dbName || targetIdField?.key || 'id';

        // Buscar el campo target (ej: Login en Usuario)
        const targetField = targetEntity.fields.toArray().find((f: any) => {
          const dbName = (f.options as any)?.dbName || f.key;
          return f.key === fieldName || dbName === fieldName;
        });

        if (!targetField) {
          return 'NULL';
        }

        const targetFieldDbName =
          (targetField.options as any)?.dbName || targetField.key;

        // Si la entidad target tiene sqlExpression (es una vista/subquery), usarla
        const targetEntityOptions = (targetEntity as any).options;
        if (targetEntityOptions?.sqlExpression) {
          let targetTableExpression =
            typeof targetEntityOptions.sqlExpression === 'function'
              ? targetEntityOptions.sqlExpression(targetEntity)
              : targetEntityOptions.sqlExpression;

          // Limpiar alias si tiene (con o sin "as", y también después de paréntesis)
          targetTableExpression = targetTableExpression
            .replace(/\s+as\s+\w+\s*$/i, '')
            .replace(/\)\s+([a-zA-Z_][a-zA-Z0-9_]*)\s*$/i, ')')
            .trim();

          return this.normalizeSql(
            `(SELECT ${this.wrapIdentifier(
              targetFieldDbName
            )} FROM ${targetTableExpression} WHERE ${this.wrapIdentifier(
              targetIdDbName
            )} = ${this.col(joinAlias, fkFieldDbName)})`
          );
        }

        // Generar subquery normal desde el contexto del JOIN actual
        return this.normalizeSql(
          `(SELECT ${this.wrapIdentifier(
            targetFieldDbName
          )} FROM ${this.wrapTableName(targetTableName)} WHERE ${this.wrapIdentifier(
            targetIdDbName
          )} = ${this.col(joinAlias, fkFieldDbName)})`
        );
      }
    );
  }

  /**
   * Método auxiliar para restaurar los valores originales
   */
  private restoreOriginalSqlExpressions(
    originalValues: Map<string, any>
  ): void {
    for (const [fieldKey, original] of originalValues) {
      const field = this.entity.fields
        .toArray()
        .find((f) => f.key === fieldKey);
      if (field) {
        const fieldOptions = (field as any).options;
        fieldOptions.sqlExpression = original.sqlExpression;
      }
    }
  }

  async find(options?: any): Promise<any[]> {
    const queryId = Math.random().toString(36).substring(7);

    // Si la entidad principal es un sqlExpression (fake entity), NO usar JOINs
    const entityOptions = (this.entity as any).options;
    if (entityOptions?.sqlExpression) {
      // Preparar campos @JOIN: para convertirlos a SUBQUERIES
      const originalSqlExpressions = await this.cleanJoinReferences();

      try {
        return await this.baseEntityProvider.find(options);
      } finally {
        this.restoreOriginalSqlExpressions(originalSqlExpressions);
      }
    }

    // Detectar si hay relaciones con defaultIncluded (solo toOne)
    const allRelations = this.detectDefaultIncludedRelations();

    const relationsToJoin = allRelations.filter((r) => {
      // Solo JOIN para relaciones toOne (no toMany)
      const isArray = r.field.valueType === Array;
      const hasFKField =
        !!r.fkFieldMetadata ||
        this.entity.fields.toArray().some((f) => f.key === r.relationInfo.field);

      // Si es Array o no tiene FK field, es toMany - NO incluir
      return !isArray && hasFKField;
    });

    if (relationsToJoin.length === 0) {
      // No hay relaciones toOne para JOIN
      // Pero si hay campos con @JOIN:, necesitamos convertirlos a subqueries
      // porque el provider base no puede manejar la sintaxis @JOIN:
      const hasJoinReferences = this.entity.fields.toArray().some((field) => {
        const fieldOptions = (field as any).options;
        if (fieldOptions?.sqlExpression) {
          // Verificar si es una función que podría devolver @JOIN:
          // Si es función, asumimos que podría devolver @JOIN: y convertimos
          // Si es string, verificar directamente
          if (typeof fieldOptions.sqlExpression === 'function') {
            try {
              const sqlExpr = fieldOptions.sqlExpression(this.entity, {
                useJoins: false,
              });
              if (sqlExpr instanceof Promise) {
                // Si es Promise, asumimos que podría tener @JOIN: y convertimos por seguridad
                return true;
              }
              return typeof sqlExpr === 'string' && sqlExpr.includes('@JOIN:');
            } catch {
              // En caso de error, asumimos que necesita conversión
              return true;
            }
          }
          return (
            typeof fieldOptions.sqlExpression === 'string' &&
            fieldOptions.sqlExpression.includes('@JOIN:')
          );
        }
        return false;
      });

      if (hasJoinReferences) {
        // Convertir @JOIN: a subqueries antes de pasar al provider base
        const originalSqlExpressions = await this.cleanJoinReferences();
        try {
          return await this.baseEntityProvider.find(options);
        } finally {
          this.restoreOriginalSqlExpressions(originalSqlExpressions);
        }
      }

      // No hay JOINs ni referencias @JOIN: - pasar directamente
      // Esto permite que entidades como PedidoCartelDetalle funcionen normalmente
      // cuando se cargan como parte de relaciones toMany, sin intentar adaptar
      // sus sqlExpression complejos (que pueden usar dbNamesOf, etc.)
      return await this.baseEntityProvider.find(options);
    }

    // HAY JOINs activos - NO convertir @JOIN: a subqueries, se resolverán en findWithJoins
    try {
      // Intentar generar query con JOINs usando Knex
      const results = await this.findWithJoins(
        relationsToJoin,
        options,
        queryId
      );
      return results;
    } catch (error) {
      console.error(`❌ [${this.entity.key}] Error al ejecutar JOINs:`, error);

      // AHORA SÍ convertir @JOIN: a subqueries para el fallback
      const originalSqlExpressions = await this.cleanJoinReferences();

      try {
        return await this.baseEntityProvider.find(options);
      } finally {
        this.restoreOriginalSqlExpressions(originalSqlExpressions);
      }
    }
  }

  /**
   * Ejecuta la query con JOINs usando Knex directamente
   */
  private async findWithJoins(
    relationsToJoin: Array<{
      fieldKey: string;
      field: any;
      relationInfo: any;
      relationFieldInfo?: any;
      relatedEntityMetadata?: EntityMetadata;
      fkFieldMetadata?: any;
    }>,
    options?: any,
    queryId?: string
  ): Promise<any[]> {
    // Acceder al Knex directamente desde el baseProvider
    const knex = this.knex;
    if (!knex) {
      throw new Error('No se pudo acceder a Knex');
    }

    // Construir la query base
    const tableName = this.entity.dbName || this.entity.key;
    const mainTableAlias = 'main';

    // Preparar información de JOINs válidos
    const validJoins: Array<{
      fieldKey: string;
      relatedEntity: EntityMetadata;
      joinAlias: string;
    }> = [];

    // Construir SELECT con alias
    const selectColumns: string[] = [];

    // Guardar campos para procesar después (necesitan saber los aliases de los JOINs)
    const deferredFields: Array<{
      field: any;
      fieldOptions: any;
      isSqlExpression: boolean;
    }> = [];

    // Agregar columnas de la tabla principal
    for (const field of this.entity.fields.toArray()) {
      const fieldOptions = field.options as any;

      // Diferir sqlExpression para procesarlos después de los JOINs (así pueden usar los aliases)
      if (fieldOptions?.sqlExpression) {
        deferredFields.push({ field, fieldOptions, isSqlExpression: true });
        continue;
      }

      // Diferir campos serverExpression para procesarlos después de los JOINs
      if (field.isServerExpression && fieldOptions?.serverExpression) {
        deferredFields.push({ field, fieldOptions, isSqlExpression: false });
        continue;
      }

      const dbName = fieldOptions?.dbName || field.key;
      selectColumns.push(
        `${this.col(mainTableAlias, dbName)} as ${this.alias(field.key)}`
      );
    }

    let query = knex(`${tableName} as ${mainTableAlias}`);

    // Función auxiliar para procesar relaciones recursivamente
    const processRelation = async (
      relation: (typeof relationsToJoin)[0],
      parentAlias: string,
      parentEntity: EntityMetadata,
      prefix: string = ''
    ): Promise<void> => {
      const relatedEntity = relation.relatedEntityMetadata;

      if (!relatedEntity) {
        return;
      }

      const currentJoinAlias = prefix
        ? `join_${prefix}_${relation.fieldKey}`
        : `join_${relation.fieldKey}`;

      // Obtener el dbName del campo FK
      const fkFieldMeta =
        relation.fkFieldMetadata ||
        parentEntity.fields
          .toArray()
          .find((f) => f.key === relation.relationInfo.field) ||
        parentEntity.fields
          .toArray()
          .find((f) => f.key === `${relation.fieldKey}Id`);

      if (!fkFieldMeta) {
        return;
      }

      const fkFieldDbName =
        (fkFieldMeta.options as any)?.dbName || fkFieldMeta.key;

      // Determinar tabla/subquery
      let relatedTableName: string;
      let usesSqlExpression = false;

      if (relatedEntity.options.sqlExpression) {
        usesSqlExpression = true;
        try {
          const sqlExpressionFn = relatedEntity.options.sqlExpression;
          let subquerySql: string;

          if (typeof sqlExpressionFn === 'function') {
            const result = sqlExpressionFn(relatedEntity);
            subquerySql = result instanceof Promise ? await result : result;
          } else {
            subquerySql = sqlExpressionFn;
          }

          subquerySql = subquerySql.trim();
          const parts = subquerySql.split(/\s+/);
          const lastPart = parts[parts.length - 1];

          if (lastPart && /^[a-zA-Z_][a-zA-Z0-9_]*$/.test(lastPart)) {
            const withoutLast = parts.slice(0, -1).join(' ');
            const openParens = (withoutLast.match(/\(/g) || []).length;
            const closeParens = (withoutLast.match(/\)/g) || []).length;

            if (openParens === closeParens) {
              subquerySql = withoutLast.trim();
            }
          }

          const isSelectSubquery = subquerySql
            .trim()
            .toUpperCase()
            .startsWith('SELECT');
          if (isSelectSubquery && !subquerySql.startsWith('(')) {
            subquerySql = `(${subquerySql})`;
          }

          relatedTableName = subquerySql;
        } catch (error) {
          return;
        }
      } else {
        relatedTableName = this.wrapTableName(
          relatedEntity.dbName || relatedEntity.key
        );
      }

      // Obtener campo ID
      const idFields = relatedEntity.idMetadata?.fields;
      if (!idFields || idFields.length === 0) {
        return;
      }

      const idFieldMetadata = idFields[0];
      const relatedIdFieldDbName =
        (idFieldMetadata.options as any)?.dbName || idFieldMetadata.key;

      // Agregar el JOIN
      const joinTableExpression = usesSqlExpression
        ? knex.raw(`${relatedTableName} as ${this.alias(currentJoinAlias)}`)
        : `${relatedTableName} as ${this.alias(currentJoinAlias)}`;

      // Verificar si el JOIN ya existe (evitar duplicados)
      const alreadyExists = validJoins.some(
        (j) => j.joinAlias === currentJoinAlias
      );

      let fieldsToInclude:
        | (string | { field: string; as: string })[]
        | undefined;

      if (!alreadyExists) {
        // Solo agregar el JOIN si no existe con el mismo alias
        query = query.leftJoin(
          joinTableExpression,
          this.col(parentAlias, fkFieldDbName),
          this.col(currentJoinAlias, relatedIdFieldDbName)
        );

        validJoins.push({
          fieldKey: prefix
            ? `${prefix}_${relation.fieldKey}`
            : relation.fieldKey,
          relatedEntity: relatedEntity,
          joinAlias: currentJoinAlias,
        });

        // Determinar campos a incluir
        const relationOptions = relation.relationInfo;
        const fieldAliases = new Map<string, string>();

        if (relationOptions.include) {
          const includeArray = Array.isArray(relationOptions.include)
            ? relationOptions.include
            : [relationOptions.include];

          fieldsToInclude = includeArray.map((item: any) => {
            if (typeof item === 'object' && item.field && item.as) {
              fieldAliases.set(item.field, item.as);
              return item.field;
            }
            return item;
          });
        }

        const includeSet = fieldsToInclude
          ? new Set(
              fieldsToInclude.map((f: any) =>
                typeof f === 'string' ? f : f.field
              )
            )
          : undefined;

        const fieldsToSelect = relatedEntity.fields.toArray().filter((f) => {
          const isRelation = (f as any)[fieldRelationInfo] !== undefined;
          if (isRelation) return false;
          const idKey = relatedEntity.idMetadata?.fields?.[0]?.key;
          const isId = f.key === idKey;
          const nameKey = (relatedEntity.options as any)?.nameKey;

          if (includeSet) {
            const explicitlyIncluded =
              includeSet.has(f.key) ||
              includeSet.has((f.options as any)?.dbName || '');
            return (
              isId ||
              explicitlyIncluded ||
              (!!nameKey && includeSet.has(nameKey) && f.key === nameKey)
            );
          }

          // Si no se especificó include, traer ID + nameKey (si existe)
          return isId || (!!nameKey && f.key === nameKey);
        });

        // Agregar campos al SELECT
        for (const field of fieldsToSelect) {
          const fieldOptions = field.options as any;
          if (fieldOptions?.sqlExpression || field.isServerExpression) {
            continue;
          }

          // Evitar IDs de relaciones anidadas (prefijo) para reducir ruido
          if (prefix && field.key === relatedEntity.idMetadata?.fields?.[0]?.key) {
            continue;
          }

          const dbName = fieldOptions?.dbName || field.key;
          const alias = fieldAliases.get(field.key);
          const columnAlias = alias || field.key;

          selectColumns.push(
            `${this.col(currentJoinAlias, dbName)} as ${this.alias(
              `${currentJoinAlias}__${columnAlias}`
            )}`
          );
        }
      }

      // Procesar relaciones anidadas explícitamente en include[]
      if (fieldsToInclude && fieldsToInclude.length > 0) {
        for (const includeItem of fieldsToInclude) {
          // Buscar si este item es una relación
          const itemField = relatedEntity.fields
            .toArray()
            .find((f) => f.key === includeItem);

          if (itemField) {
            const itemRelationInfo = (itemField as any)[fieldRelationInfo];

            if (itemRelationInfo && itemRelationInfo.toRepo?.metadata) {
              // Es una relación - crear un JOIN anidado
              const nestedEntityMetadata = itemRelationInfo.toRepo.metadata;
              const nestedRelation = {
                fieldKey: itemField.key,
                field: itemField,
                relationInfo: itemField.options as any,
                relationFieldInfo: itemRelationInfo,
                relatedEntityMetadata: nestedEntityMetadata,
                fkFieldMetadata: this.getRelationDetails(
                  relatedEntity,
                  itemField.key
                ).fkField,
                nestedRelations: [] as any[],
              };

              const newPrefix = prefix
                ? `${prefix}_${relation.fieldKey}`
                : relation.fieldKey;
              await processRelation(
                nestedRelation,
                currentJoinAlias,
                relatedEntity,
                newPrefix
              );
            }
          }
        }
      }

      // Procesar relaciones anidadas recursivamente (desde detectNestedPathsInSqlExpressions)
      const nestedRels = (relation as any).nestedRelations;
      if (nestedRels && nestedRels.length > 0) {
        for (const nestedRel of nestedRels) {
          const newPrefix = prefix
            ? `${prefix}_${relation.fieldKey}`
            : relation.fieldKey;
          await processRelation(
            nestedRel,
            currentJoinAlias,
            relatedEntity,
            newPrefix
          );
        }
      }
    };

    // Agregar los JOINs para cada relación
    for (const relation of relationsToJoin) {
      // Procesar la relación y sus anidadas
      await processRelation(relation, mainTableAlias, this.entity);
    }

    // Procesar campos diferidos (después de tener los JOINs construidos)
    for (const { field, fieldOptions, isSqlExpression } of deferredFields) {
      if (isSqlExpression) {
        // Procesar sqlExpression - ahora tiene acceso a los JOINs
        let sqlExpr =
          typeof fieldOptions.sqlExpression === 'function'
            ? fieldOptions.sqlExpression(this.entity, { useJoins: true })
            : fieldOptions.sqlExpression;

        // IMPORTANTE: Esperar si es una Promise (ej: async sqlExpression que usa await dbNamesOf)
        if (sqlExpr instanceof Promise) {
          sqlExpr = await sqlExpr;
        }

        if (typeof sqlExpr === 'string') {
          // Limpiar el SQL: remover saltos de línea y espacios múltiples
          let cleanSql = sqlExpr.replace(/\s+/g, ' ').trim();

          // DETECTAR CONVENCIÓN @JOIN:relation.field o @JOIN:path.with.multiple.parts.field
          const joinConventionMatch = cleanSql.match(/^@JOIN:([\w.]+)$/);

          if (joinConventionMatch) {
            const fullPath = joinConventionMatch[1];
            const parts = fullPath.split('.');

            // El último elemento es el campo, el resto es el path
            const fieldName = parts[parts.length - 1];
            const relationPath = parts.slice(0, -1);

            // Construir el alias del JOIN según el path
            // Ej: ['fichaTasador', 'tasador'] → 'join_fichaTasador_tasador'
            const joinAliasToFind =
              relationPath.length > 0 ? `join_${relationPath.join('_')}` : null;

            if (!joinAliasToFind) {
              cleanSql = 'NULL';
            } else {
              // Buscar el JOIN correspondiente por alias
              const matchingJoin = validJoins.find(
                (j) => j.joinAlias === joinAliasToFind
              );

              if (matchingJoin) {
                // Buscar el campo en la entidad relacionada (buscar por key o por dbName)
                const relatedField = matchingJoin.relatedEntity.fields
                  .toArray()
                  .find((f: any) => {
                    const dbName = (f.options as any)?.dbName || f.key;
                    return f.key === fieldName || dbName === fieldName;
                  });

                if (relatedField) {
                  const relFieldOptions = relatedField.options as any;

                  // Si el campo target tiene sqlExpression, resolverlo como subquery anidado
                  if (relFieldOptions?.sqlExpression) {
                    // Evaluar el sqlExpression del campo target
                    let targetSqlExpr =
                      typeof relFieldOptions.sqlExpression === 'function'
                        ? relFieldOptions.sqlExpression(
                            matchingJoin.relatedEntity,
                            {
                              useJoins: false,
                            }
                          )
                        : relFieldOptions.sqlExpression;

                    // Esperar si es Promise
                    if (targetSqlExpr instanceof Promise) {
                      targetSqlExpr = await targetSqlExpr;
                    }

                    if (typeof targetSqlExpr === 'string') {
                      // Convertir @JOIN:X.Y a subquery desde el contexto del JOIN actual
                      cleanSql = this.convertJoinToSubqueryFromJoinContext(
                        targetSqlExpr,
                        relatedField.key,
                        matchingJoin.joinAlias,
                        matchingJoin.relatedEntity
                      );
                    } else {
                      cleanSql = 'NULL';
                    }
                  } else {
                    // Campo físico normal
                    const relFieldDbName = this.getFieldDbName(relatedField);
                    cleanSql = this.col(matchingJoin.joinAlias, relFieldDbName);
                  }
                } else {
                  cleanSql = 'NULL';
                }
              } else {
                cleanSql = 'NULL';
              }
            }
          } else {
            // SQL normal - aplicar transformaciones de alias
            // Reemplazar tanto [TableName]. como TableName. (sin corchetes)
            const tableNameRegex1 = new RegExp(`\\[${tableName}\\]\\.`, 'g');
            const tableNameRegex2 = new RegExp(`\\b${tableName}\\.`, 'g');
            cleanSql = cleanSql.replace(tableNameRegex1, `${mainTableAlias}.`);
            cleanSql = cleanSql.replace(tableNameRegex2, `${mainTableAlias}.`);

            // También reemplazar el entityKey si es diferente (ej: PedidoCartelDetalle vs PedidoCartelDetalle)
            const entityKey = this.entity.key;
            if (entityKey !== tableName) {
              const entityKeyRegex1 = new RegExp(`\\[${entityKey}\\]\\.`, 'g');
              const entityKeyRegex2 = new RegExp(`\\b${entityKey}\\.`, 'g');
              cleanSql = cleanSql.replace(
                entityKeyRegex1,
                `${mainTableAlias}.`
              );
              cleanSql = cleanSql.replace(
                entityKeyRegex2,
                `${mainTableAlias}.`
              );
            }

            // Reemplazar columnas individuales sin prefijo de tabla
            cleanSql = cleanSql.replace(
              /\[([^\]]+)\]/g,
              (match, columnName) => {
                const beforeMatch = cleanSql.substring(
                  0,
                  cleanSql.indexOf(match)
                );
                // Si ya tiene un prefijo de tabla (algo seguido de punto antes), no tocar
                if (beforeMatch.match(/\w+\s*\.\s*$/)) {
                  return match;
                }
                // Buscar si es un campo de la tabla principal
                const mainField = this.entity.fields
                  .toArray()
                  .find((f: any) => {
                    const dbName = (f.options as any)?.dbName || f.key;
                    return dbName === columnName || f.key === columnName;
                  });
                if (mainField) {
                  return `${mainTableAlias}.${match}`;
                }
                return match;
              }
            );
          }

          selectColumns.push(
            `${this.normalizeSql(cleanSql)} as ${this.alias(field.key)}`
          );
        }
      } else {
        // Procesar serverExpression - intentar auto-generar SQL
        const autoSql = this.tryGenerateSqlFromServerExpression(
          field.key,
          fieldOptions.serverExpression,
          validJoins
        );

        if (autoSql) {
          selectColumns.push(
            `${this.normalizeSql(autoSql)} as ${this.alias(field.key)}`
          );
        }
        // Si no se pudo auto-generar, se calculará con serverExpression después
      }
    }

    // Aplicar el SELECT con alias
    query = query.select(knex.raw(selectColumns.join(', ')));

    // Aplicar filtros si existen
    if (options?.where) {
      try {
        const filterBuilder = new FilterToKnexBridge(
          this.entity,
          mainTableAlias,
          validJoins,
          knex
        );
        options.where.__applyToConsumer(filterBuilder);
        const whereConditions = await filterBuilder.resolveWhere();

        if (whereConditions.length > 0) {
          query = query.where((b) => whereConditions.forEach((fn) => fn(b)));
        }
      } catch (error) {
        console.error(`❌ Error aplicando filtros:`, error);
      }
    }

    // Aplicar paginación
    if (options?.limit) {
      query = query.limit(options.limit);
      if (options.page) {
        query = query.offset((options.page - 1) * options.limit);
      }
    }

    // Aplicar ordenamiento
    if (!options.orderBy) {
      options.orderBy = Sort.createUniqueSort(this.entity, new Sort());
    }
    if (options.orderBy) {
      try {
        // Crear un FilterToKnexBridge temporal para resolver sqlExpression en ORDER BY
        const orderByBridge = new FilterToKnexBridge(
          this.entity,
          mainTableAlias,
          validJoins,
          knex
        );
        const orderByColumns = await Promise.all(
          options.orderBy.Segments.map(async (segment: any) => {
            // Intentar obtener el sqlExpression completo (maneja @JOIN y sqlExpression async)
            const columnName = await orderByBridge.getColumnNameAsync(
              segment.field
            );

            if (!columnName) {
              // Si no se pudo resolver, usar dbName simple
              const dbName = this.getFieldDbName(segment.field);
              const fallbackColumnName = dbName.includes('.')
                ? dbName
                : `${mainTableAlias}.${dbName}`;
              return {
                column: knex.raw(fallbackColumnName),
                order: segment.isDescending
                  ? ('desc' as const)
                  : ('asc' as const),
              };
            }

            return {
              column: knex.raw(columnName),
              order: segment.isDescending
                ? ('desc' as const)
                : ('asc' as const),
            };
          })
        );
        query = query.orderBy(orderByColumns);
      } catch (error) {
        console.error(`❌ Error aplicando orderBy:`, error);
      }
    }

    const sqlDebug = query.toSQL();

    if (SqlDatabase.LogToConsole) {
      console.log(`\n📝 SQL [${this.entity.key}]:\n${sqlDebug.sql}`);
      if (sqlDebug.bindings && sqlDebug.bindings.length > 0) {
        console.log(`   Params:`, sqlDebug.bindings);
      }
    }

    // Ejecutar la query
    const rawResults = await query;
    // Mapear los resultados a objetos anidados
    const results = this.mapJoinedResults(rawResults, validJoins);

    // NO cargamos toMany aquí - dejamos que Remult lo haga automáticamente
    // El Repository de Remult cargará las relaciones toMany después
    return results;
  }

  /**
   * Mapea los resultados planos del JOIN a objetos con relaciones anidadas
   */
  private mapJoinedResults(
    rawResults: any[],
    validJoins: Array<{
      fieldKey: string;
      relatedEntity: EntityMetadata;
      joinAlias: string;
    }>
  ): any[] {
    if (!rawResults || rawResults.length === 0) {
      return [];
    }

    return rawResults.map((row) => {
      const mainEntity: any = {};

      // Extraer campos de la entidad principal (solo los que vinieron en el SELECT)
      for (const key in row) {
        if (!key.includes('__')) {
          const field = this.entity.fields.toArray().find((f) => f.key === key);

          if (field) {
            try {
              // Aplicar conversión de tipo desde BD a JS
              mainEntity[key] = field.valueConverter.fromDb(row[key]);
            } catch (err) {
              throw new Error(`Failed to load from db: ${key}\r\n${err}`);
            }
          } else {
            // Campo no encontrado en metadata (posiblemente sqlExpression calculado)
            // Asignar directamente
            mainEntity[key] = row[key];
          }
        }
      }

      // Construir objetos anidados para cada relación
      for (const join of validJoins) {
        const relatedObject: any = {};
        const prefix = `${join.joinAlias}__`;
        let hasData = false;

        // Extraer campos de la entidad relacionada
        for (const key in row) {
          if (key.startsWith(prefix)) {
            const fieldKey = key.substring(prefix.length);
            const relatedField = join.relatedEntity.fields
              .toArray()
              .find((f) => f.key === fieldKey);

            if (relatedField) {
              try {
                // Aplicar conversión de tipo desde BD a JS
                relatedObject[fieldKey] = relatedField.valueConverter.fromDb(
                  row[key]
                );
                if (row[key] !== null && row[key] !== undefined) {
                  hasData = true;
                }
              } catch (err) {
                throw new Error(
                  `Failed to load from db: ${join.joinAlias}.${fieldKey}\r\n${err}`
                );
              }
            } else {
              // Campo no encontrado (posiblemente sqlExpression calculado)
              relatedObject[fieldKey] = row[key];
              if (row[key] !== null && row[key] !== undefined) {
                hasData = true;
              }
            }
          }
        }

        // Detectar si es un JOIN anidado (ej: fichaTasador_tasador)
        // y asignarlo correctamente en la estructura anidada
        if (join.fieldKey.includes('_')) {
          // JOIN anidado - ej: "fichaTasador_tasador"
          const parts = join.fieldKey.split('_');

          // Navegar/crear la estructura anidada
          let target: any = mainEntity;
          for (let i = 0; i < parts.length - 1; i++) {
            if (!target[parts[i]]) {
              target[parts[i]] = {};
            }
            target = target[parts[i]];
          }

          // Asignar el objeto al último nivel
          const lastKey = parts[parts.length - 1];
          target[lastKey] = hasData ? relatedObject : null;
        } else {
          // JOIN normal de primer nivel
          mainEntity[join.fieldKey] = hasData ? relatedObject : null;
        }
      }

      // Calcular campos con serverExpression que dependan de las relaciones
      for (const field of this.entity.fields.toArray()) {
        const fieldOptions = field.options as any;

        // Si tiene serverExpression, intentar calcularlo
        if (
          fieldOptions?.serverExpression &&
          typeof fieldOptions.serverExpression === 'function'
        ) {
          try {
            const value = fieldOptions.serverExpression(mainEntity);
            // Solo asignar si no es una promesa (serverExpression síncronos)
            if (value !== undefined && !(value instanceof Promise)) {
              mainEntity[field.key] = value;
            }
          } catch (e) {
            // Ignorar errores en serverExpression
          }
        }
      }

      return mainEntity;
    });
  }

  async groupBy(options?: any): Promise<any[]> {
    return this.baseEntityProvider.groupBy(options);
  }

  /**
   * Implementar count con JOINs
   */
  async count(where: Filter): Promise<number> {
    // Si la entidad principal es un sqlExpression (fake entity), NO usar JOINs
    const entityOptions = (this.entity as any).options;
    if (entityOptions?.sqlExpression) {
      return this.baseEntityProvider.count(where);
    }

    const allRelations = this.detectDefaultIncludedRelations();
    const relationsToJoin = allRelations.filter((r) => {
      const isArray = r.field.valueType === Array;
      const hasFKField =
        !!r.fkFieldMetadata ||
        this.entity.fields.toArray().some((f) => f.key === r.relationInfo.field);

      if (isArray || !hasFKField) {
        return false;
      }

      return true;
    });

    if (relationsToJoin.length === 0) {
      return this.baseEntityProvider.count(where);
    }

    const countId = Math.random().toString(36).substring(7);

    try {
      const count = await this.countWithJoins(relationsToJoin, where, countId);
      return count;
    } catch (error) {
      console.error(
        `❌ [${this.entity.key}] Error al ejecutar COUNT con JOINs:`,
        error
      );
      return this.baseEntityProvider.count(where);
    }
  }

  async update(id: any, data: any): Promise<any> {
    const originalSqlExpressions = await this.cleanJoinReferences();
    try {
      return await this.baseEntityProvider.update(id, data);
    } finally {
      this.restoreOriginalSqlExpressions(originalSqlExpressions);
    }
  }

  async delete(id: any): Promise<void> {
    const originalSqlExpressions = await this.cleanJoinReferences();
    try {
      return await this.baseEntityProvider.delete(id);
    } finally {
      this.restoreOriginalSqlExpressions(originalSqlExpressions);
    }
  }

  async insert(data: any): Promise<any> {
    const originalSqlExpressions = await this.cleanJoinReferences();
    try {
      return await this.baseEntityProvider.insert(data);
    } finally {
      this.restoreOriginalSqlExpressions(originalSqlExpressions);
    }
  }

  /**
   * Ejecuta COUNT con JOINs
   */
  private async countWithJoins(
    relationsToJoin: Array<{
      fieldKey: string;
      field: any;
      relationInfo: any;
      relationFieldInfo?: any;
      relatedEntityMetadata?: EntityMetadata;
      fkFieldMetadata?: any;
    }>,
    where?: Filter,
    countId?: string
  ): Promise<number> {
    const knex = this.knex;
    if (!knex) {
      throw new Error('No se pudo acceder a Knex');
    }

    const tableName = this.entity.dbName || this.entity.key;
    const mainTableAlias = 'main';

    let query = knex(`${tableName} as ${mainTableAlias}`);

    const validJoins: Array<{
      fieldKey: string;
      relatedEntity: EntityMetadata;
      joinAlias: string;
    }> = [];

    // Construir los JOINs (necesario para los filtros)
    for (const relation of relationsToJoin) {
      const fkFieldKey = relation.relationInfo.field;
      const relatedEntity = relation.relatedEntityMetadata;

      if (!relatedEntity) {
        continue;
      }

      const fkFieldMeta =
        relation.fkFieldMetadata ||
        this.entity.fields.toArray().find((f) => f.key === fkFieldKey) ||
        this.entity.fields
          .toArray()
          .find((f) => f.key === `${relation.fieldKey}Id`);
      const fkFieldDbName =
        (fkFieldMeta?.options as any)?.dbName || fkFieldMeta?.key || fkFieldKey;

      let relatedTableName: string;
      let usesSqlExpression = false;

      if (relatedEntity.options.sqlExpression) {
        usesSqlExpression = true;

        try {
          const sqlExpressionFn = relatedEntity.options.sqlExpression;
          let subquerySql: string;

          if (typeof sqlExpressionFn === 'function') {
            // @ts-expect-error - Pasamos contexto adicional que Remult no define en tipos
            subquerySql = await sqlExpressionFn(relatedEntity, {
              useJoins: true,
            });
          } else {
            subquerySql = sqlExpressionFn;
          }

          subquerySql = subquerySql.trim();

          const parts = subquerySql.split(/\s+/);
          const lastPart = parts[parts.length - 1];

          if (lastPart && /^[a-zA-Z_][a-zA-Z0-9_]*$/.test(lastPart)) {
            const withoutLast = parts.slice(0, -1).join(' ');
            const openParens = (withoutLast.match(/\(/g) || []).length;
            const closeParens = (withoutLast.match(/\)/g) || []).length;

            if (openParens === closeParens) {
              subquerySql = withoutLast.trim();
            }
          }

          const isSelectSubquery = subquerySql
            .trim()
            .toUpperCase()
            .startsWith('SELECT');
          if (isSelectSubquery && !subquerySql.startsWith('(')) {
            subquerySql = `(${subquerySql})`;
          }

          relatedTableName = subquerySql;
        } catch (error) {
          continue;
        }
      } else {
        relatedTableName = this.wrapTableName(
          relatedEntity.dbName || relatedEntity.key
        );
      }

      const idFields = relatedEntity.idMetadata?.fields;
      if (!idFields || idFields.length === 0) {
        continue;
      }

      const idFieldMetadata = idFields[0];
      const relatedIdFieldDbName =
        (idFieldMetadata.options as any)?.dbName || idFieldMetadata.key;

      const joinAlias = `join_${relation.fieldKey}`;

      const joinTableExpression = usesSqlExpression
        ? knex.raw(`${relatedTableName} as ${this.alias(joinAlias)}`)
        : `${relatedTableName} as ${this.alias(joinAlias)}`;

      query = query.leftJoin(
        joinTableExpression,
        this.col(mainTableAlias, fkFieldDbName),
        this.col(joinAlias, relatedIdFieldDbName)
      );

      validJoins.push({
        fieldKey: relation.fieldKey,
        relatedEntity,
        joinAlias,
      });
    }

    // Aplicar filtros si existen
    if (where) {
      try {
        const filterBuilder = new FilterToKnexBridge(
          this.entity,
          mainTableAlias,
          validJoins,
          knex
        );
        where.__applyToConsumer(filterBuilder);
        const whereConditions = await filterBuilder.resolveWhere();

        if (whereConditions.length > 0) {
          query = query.where((b) => whereConditions.forEach((fn) => fn(b)));
        }
      } catch (error) {
        console.error(`❌ Error aplicando filtros en COUNT:`, error);
      }
    }

    // Hacer el COUNT
    query = query.count('* as count');

    const sqlDebug = query.toSQL();
    if (SqlDatabase.LogToConsole) {
      console.log(`\n📝 COUNT SQL [${this.entity.key}]:\n${sqlDebug.sql}`);
      if (sqlDebug.bindings && sqlDebug.bindings.length > 0) {
        console.log(`   Params:`, sqlDebug.bindings);
      }
    }

    const result = await query;
    const count = parseInt(result[0].count as string, 10);
    return count;
  }

  /**
   * Helper para obtener el nombre de base de datos de un field
   */
  private getFieldDbName(field: any): string {
    const fieldOptions = field.options as any;
    return fieldOptions?.dbName || field.key;
  }

  /**
   * Intenta generar un sqlExpression automáticamente desde un serverExpression
   * Detecta patrones como: (entity) => entity.relation?.field
   */
  private tryGenerateSqlFromServerExpression(
    fieldKey: string,
    serverExpression: Function,
    validJoins: Array<{
      fieldKey: string;
      relatedEntity: EntityMetadata;
      joinAlias: string;
    }>
  ): string | null {
    try {
      // Convertir la función a string para analizarla
      const fnString = serverExpression.toString();

      // Patrón común: (entity) => entity.relation?.field
      // Buscar: entity.RELATIONNAME?.FIELDNAME
      const match = fnString.match(/\w+\.(\w+)\?\.(\w+)/);

      if (!match) return null;

      const [, relationKey, targetFieldKey] = match;

      // Buscar el JOIN correspondiente por alias
      const join = validJoins.find(
        (j) => j.joinAlias === `join_${relationKey}`
      );
      if (!join) return null;

      // Buscar el field en la entidad relacionada
      const targetField = join.relatedEntity.fields
        .toArray()
        .find((f) => f.key === targetFieldKey);
      if (!targetField) return null;

      // Obtener el dbName del campo
      const targetFieldOptions = targetField.options as any;

      // Si el campo target es un campo calculado (tiene sqlExpression o serverExpression),
      // NO intentar auto-generar SQL porque ya fue resuelto en el JOIN
      if (targetFieldOptions?.sqlExpression || targetField.isServerExpression) {
        return null;
      }

      const targetDbName = targetFieldOptions?.dbName || targetField.key;

      // Generar el SQL: joinAlias.[FieldDbName]
      return this.col(join.joinAlias, targetDbName);
    } catch (error) {
      // Si falla el parsing, simplemente no generamos SQL automático
      return null;
    }
  }

  private detectDefaultIncludedRelations(): Array<{
    fieldKey: string;
    field: any;
    relationInfo: any;
    relationFieldInfo?: any;
    relatedEntityMetadata?: EntityMetadata;
    fkFieldMetadata?: any;
  }> {
    const relations: Array<{
      fieldKey: string;
      field: any;
      relationInfo: any;
      relationFieldInfo?: any;
      relatedEntityMetadata?: EntityMetadata;
      fkFieldMetadata?: any;
    }> = [];

    for (const field of this.entity.fields) {
      const relationDetails = this.getRelationDetails(this.entity, field.key);
      if (!relationDetails.relationInfo || !relationDetails.relatedEntity) {
        continue;
      }

      const fieldOptions = field.options as any;
      // Por ahora incluimos todas las relaciones toOne para habilitar JOINs
      const includeRelation = true || fieldOptions?.defaultIncluded === true;
      if (!includeRelation) continue;

      relations.push({
        fieldKey: field.key,
        field,
        relationInfo: fieldOptions,
        relationFieldInfo: relationDetails.relationInfo,
        relatedEntityMetadata: relationDetails.relatedEntity,
        fkFieldMetadata: relationDetails.fkField,
      });
    }

    // Detectar paths anidados en sqlExpression y agregar los JOINs necesarios
    const nestedPaths = this.detectNestedPathsInSqlExpressions();
    for (const path of nestedPaths) {
      // path es algo como ['reserva', 'fichaTasador', 'tasador']
      this.addNestedRelationsToJoin(path, relations);
    }

    return relations;
  }

  /**
   * Detecta paths anidados en sqlExpression de los campos
   * Busca patrones como @JOIN:reserva.fichaTasador.tasador.Login
   */
  private detectNestedPathsInSqlExpressions(): string[][] {
    const paths: string[][] = [];
    const seenPaths = new Set<string>();

    for (const field of this.entity.fields) {
      const fieldOptions = (field as any).options;
      if (fieldOptions?.sqlExpression) {
        let sqlExpr =
          typeof fieldOptions.sqlExpression === 'function'
            ? fieldOptions.sqlExpression(this.entity, { useJoins: true })
            : fieldOptions.sqlExpression;

        if (typeof sqlExpr === 'string') {
          // Buscar patrones @JOIN:path.to.field
          const matches = sqlExpr.matchAll(/@JOIN:([\w.]+)/g);
          for (const match of matches) {
            const fullPath = match[1]; // ej: "reserva.fichaTasador.tasador.Login"
            const parts = fullPath.split('.');

            if (parts.length > 2) {
              // Es un path anidado (más de 2 partes: relación + campo)
              // Extraer solo el path de relaciones (sin el último que es el campo)
              const relationPath = parts.slice(0, -1); // ['reserva', 'fichaTasador', 'tasador']
              const pathKey = relationPath.join('.');

              if (!seenPaths.has(pathKey)) {
                seenPaths.add(pathKey);
                paths.push(relationPath);
              }
            }
          }
        }
      }
    }

    return paths;
  }

  /**
   * Agrega las relaciones intermedias necesarias para un path anidado
   * Por ejemplo, para ['reserva', 'fichaTasador', 'tasador']:
   * - Asegura que 'reserva' esté en relations
   * - Agrega 'fichaTasador' como relación anidada de 'reserva'
   * - Agrega 'tasador' como relación anidada de 'fichaTasador'
   */
  private addNestedRelationsToJoin(
    path: string[],
    relations: Array<{
      fieldKey: string;
      field: any;
      relationInfo: any;
      relationFieldInfo?: any;
      relatedEntityMetadata?: EntityMetadata;
      fkFieldMetadata?: any;
      nestedRelations?: Array<{
        fieldKey: string;
        field: any;
        relationInfo: any;
        relationFieldInfo?: any;
        relatedEntityMetadata?: EntityMetadata;
        fkFieldMetadata?: any;
      }>;
    }>
  ): void {
    let currentEntity = this.entity;
    let currentRelations = relations;

    for (let i = 0; i < path.length; i++) {
      const relationKey = path[i];
      const fkFieldKey = `${relationKey}Id`;

      // Buscar si ya existe esta relación en el nivel actual
      let existingRelation = currentRelations.find(
        (r) => r.fieldKey === relationKey
      );

      if (!existingRelation) {
        // No existe, hay que crearla
        const relationField = currentEntity.fields
          .toArray()
          .find((f) => f.key === relationKey);
        const relationDetails = this.getRelationDetails(
          currentEntity,
          relationKey
        );
        const fkField =
          relationDetails.fkField ||
          currentEntity.fields.toArray().find((f) => f.key === fkFieldKey);

        if (!relationField || !fkField) {
          // No se pudo encontrar la relación, abortar
          return;
        }

        const relationFieldInfo = (relationField as any)[fieldRelationInfo];
        if (!relationFieldInfo) {
          return;
        }

        const fieldOptions = relationField.options as any;
        let relatedEntityMetadata: EntityMetadata | undefined;

        try {
          if (relationFieldInfo.toRepo && relationFieldInfo.toRepo.metadata) {
            relatedEntityMetadata = relationFieldInfo.toRepo.metadata;
          }
        } catch (e) {
          // Error obteniendo metadata
          return;
        }

        existingRelation = {
          fieldKey: relationField.key,
          field: relationField,
          relationInfo: fieldOptions,
          relationFieldInfo: relationFieldInfo,
          relatedEntityMetadata,
          nestedRelations: [],
          fkFieldMetadata: fkField,
        };

        currentRelations.push(existingRelation);
      }

      if (!existingRelation) {
        return;
      }

      // Preparar para el siguiente nivel
      if (i < path.length - 1) {
        // No es el último elemento, seguir navegando
        if (!existingRelation.nestedRelations) {
          existingRelation.nestedRelations = [];
        }
        currentRelations = existingRelation.nestedRelations;
        currentEntity = existingRelation.relatedEntityMetadata!;
      }
    }
  }
}

/**
 * Factory function para crear el provider optimizado
 */
/**
 * Clase para convertir filtros de Remult a condiciones Knex WHERE
 * Basada en FilterConsumerBridgeToKnexRequest de Remult
 */
class FilterToKnexBridge {
  private promises: Promise<void>[] = [];
  private result: ((builder: Knex.QueryBuilder) => void)[] = [];
  private _addWhere = true;
  private knex?: Knex;

  constructor(
    private entityMetadata: EntityMetadata,
    private tableAlias: string,
    private validJoins: Array<{
      fieldKey: string;
      relatedEntity: EntityMetadata;
      joinAlias: string;
    }> = [],
    knexInstance?: Knex
  ) {
    this.knex = knexInstance;
  }

  private wrapIdentifier(id: string): string {
    if (!id) return id;
    if (id.startsWith('[') || id.startsWith('"')) return id;
    if (id.includes('(') || id.includes(' ')) {
      return id;
    }
    const dialect = (this.knex as any)?.client?.config?.client;
    if (dialect === 'mssql') {
      return `[${id}]`;
    }
    const escaped = id.replace(/"/g, '""');
    return `"${escaped}"`;
  }

  private col(tableAlias: string, column: string): string {
    return `${this.wrapIdentifier(tableAlias)}.${this.wrapIdentifier(column)}`;
  }

  private wrapTableName(name: string): string {
    if (!name) return name;
    if (name.includes('(')) return name;
    if (name.includes('.')) {
      return name
        .split('.')
        .map((part) => this.wrapIdentifier(part))
        .join('.');
    }
    return this.wrapIdentifier(name);
  }

  private normalizeSql(sql: string): string {
    return sql
      .replace(/\[([^\]]+)\]/g, (_m, id) =>
      this.wrapIdentifier(id as string)
      )
      .replace(/"main"\."main"\./g, '"main".')
      .replace(/\[main\]\.\[main\]\./g, '[main].');
  }

  async resolveWhere(): Promise<((builder: Knex.QueryBuilder) => void)[]> {
    while (this.promises.length > 0) {
      const p = this.promises;
      this.promises = [];
      for (const pr of p) {
        await pr;
      }
    }
    return this.result;
  }

  custom(key: string, customItem: any): void {
    throw new Error('Custom filter should be translated before it gets here');
  }

  or(orElements: Filter[]): void {
    this.promises.push(
      (async () => {
        const result: ((builder: Knex.QueryBuilder) => void)[] = [];
        for (const element of orElements) {
          const f = new FilterToKnexBridge(
            this.entityMetadata,
            this.tableAlias,
            this.validJoins,
            this.knex
          );
          f._addWhere = false;
          element.__applyToConsumer(f);
          const where = await f.resolveWhere();
          if (where.length > 0) {
            result.push((b) => {
              b.orWhere((b) => {
                where.forEach((x) => x(b));
              });
            });
          } else return; // empty or means all rows
        }
        if (result.length > 0) {
          this.result.push((b) => b.where((x) => result.find((y) => y(x))));
        }
      })()
    );
  }

  not(element: Filter): void {
    this.promises.push(
      (async () => {
        const f = new FilterToKnexBridge(
          this.entityMetadata,
          this.tableAlias,
          this.validJoins,
          this.knex
        );
        f._addWhere = false;
        element.__applyToConsumer(f);
        const where = await f.resolveWhere();
        if (where.length > 0) {
          this.result.push((b) => {
            b.whereNot((b) => {
              where.forEach((x) => x(b));
            });
          });
        } else return; // empty or means all rows
      })()
    );
  }

  isNull(col: any): void {
    this.promises.push(
      (async () => {
        const colName = await this.getColumnNameAsync(col);
        if (!colName) return; // Si no se pudo generar SQL, saltar
        this.result.push((b) => b.whereNull(colName));
      })()
    );
  }

  isNotNull(col: any): void {
    this.promises.push(
      (async () => {
        const colName = await this.getColumnNameAsync(col);
        if (!colName) return; // Si no se pudo generar SQL, saltar
        this.result.push((b) => b.whereNotNull(colName));
      })()
    );
  }

  isIn(col: any, val: any[]): void {
    this.promises.push(
      (async () => {
        const colName = await this.getColumnNameAsync(col);
        if (!colName) return;
        this.result.push((knex) =>
          knex.whereIn(
            colName,
            val.map((x) => this.translateValue(col, x))
          )
        );
      })()
    );
  }

  isEqualTo(col: any, val: any): void {
    this.add(col, val, '=');
  }

  isDifferentFrom(col: any, val: any): void {
    this.add(col, val, '<>');
  }

  isGreaterOrEqualTo(col: any, val: any): void {
    this.add(col, val, '>=');
  }

  isGreaterThan(col: any, val: any): void {
    this.add(col, val, '>');
  }

  isLessOrEqualTo(col: any, val: any): void {
    this.add(col, val, '<=');
  }

  isLessThan(col: any, val: any): void {
    this.add(col, val, '<');
  }

  containsCaseInsensitive(col: any, val: any): void {
    this.promises.push(
      (async () => {
        const colName = await this.getColumnNameAsync(col);
        if (!colName) return; // Si no se pudo generar SQL o se debe ignorar, saltar

        // Reemplazar espacios por % para búsquedas más flexibles
        // Ej: "pedido 13" → "pedido%13" encontrará "Pedido 00013"
        const searchPattern = val.replace(/\s+/g, '%'); // Reemplazar espacios por %
        const finalPattern = `%${searchPattern}%`;

        this.result.push((b) =>
          b.orWhereRaw(`lower (${colName}) like lower (?)`, [finalPattern])
        );
      })()
    );
  }

  notContainsCaseInsensitive(col: any, val: any): void {
    this.promises.push(
      (async () => {
        const colName = await this.getColumnNameAsync(col);
        if (!colName) return; // Si no se pudo generar SQL o se debe ignorar, saltar
        this.result.push((b) =>
          b.whereRaw(`not lower (${colName}) like lower (?)`, [
            `%${val.replace(/'/g, "''")}%`,
          ])
        );
      })()
    );
  }

  startsWithCaseInsensitive(col: any, val: any): void {
    this.promises.push(
      (async () => {
        const colName = await this.getColumnNameAsync(col);
        if (!colName) return; // Si no se pudo generar SQL, saltar
        this.result.push((b) =>
          b.whereRaw(`lower (${colName}) like lower (?)`, [
            `${val.replace(/'/g, "''")}%`,
          ])
        );
      })()
    );
  }

  endsWithCaseInsensitive(col: any, val: any): void {
    this.promises.push(
      (async () => {
        const colName = await this.getColumnNameAsync(col);
        if (!colName) return; // Si no se pudo generar SQL, saltar
        this.result.push((b) =>
          b.whereRaw(`lower (${colName}) like lower (?)`, [
            `%${val.replace(/'/g, "''")}`,
          ])
        );
      })()
    );
  }

  databaseCustom(databaseCustom: any): void {
    this.promises.push(
      (async () => {
        if (databaseCustom?.buildKnex) {
          this.result.push(await databaseCustom.buildKnex());
        }
        // buildSql no está implementado aquí por simplicidad
      })()
    );
  }

  private add(col: any, val: any, operator: string): void {
    this.promises.push(
      (async () => {
        const colName = await this.getColumnNameAsync(col);
        if (!colName) return; // Si no se pudo generar SQL o se debe ignorar, saltar
        this.result.push((b) => {
          const translateValueResult = this.translateValue(col, val);
          const x = b.where(colName, operator, translateValueResult);
          return x;
        });
      })()
    );
  }

  async getColumnNameAsync(col: any): Promise<string | null> {
    const fieldOptions = col.options as any;

    // Si es un campo con sqlExpression, intentar optimizarlo con JOINs existentes
    if (fieldOptions?.sqlExpression) {
      let sqlExpr =
        typeof fieldOptions.sqlExpression === 'function'
          ? fieldOptions.sqlExpression(this.entityMetadata, {
              useJoins: this.validJoins.length > 0,
            })
          : fieldOptions.sqlExpression;

      // Esperar si es una Promise
      if (sqlExpr instanceof Promise) {
        sqlExpr = await sqlExpr;
      }

      if (typeof sqlExpr === 'string') {
        let cleanSql = sqlExpr.replace(/\s+/g, ' ').trim();

        // DETECTAR CONVENCIÓN @JOIN:relation.field o paths anidados
        const joinConventionMatch = cleanSql.match(/^@JOIN:([\w.]+)$/);

        if (joinConventionMatch) {
          const fullPath = joinConventionMatch[1];
          const parts = fullPath.split('.');
          if (parts.length >= 2) {
            const fieldName = parts.pop() as string;
            const relationPath = parts;
            const joinAliasToFind =
              relationPath.length > 0
                ? `join_${relationPath.join('_')}`
                : null;

            if (joinAliasToFind) {
              const matchingJoin = this.validJoins.find(
                (j) => j.joinAlias === joinAliasToFind
              );

              if (matchingJoin) {
                const relatedField = matchingJoin.relatedEntity.fields
                  .toArray()
                  .find((f: any) => {
                    const dbName = (f.options as any)?.dbName || f.key;
                    return f.key === fieldName || dbName === fieldName;
                  });

                if (relatedField) {
                  const relFieldDbName =
                    (relatedField.options as any)?.dbName || relatedField.key;
                  return this.col(matchingJoin.joinAlias, relFieldDbName);
                }
                return null;
              }
            }

            return this.convertJoinToSubquery(cleanSql, col.key, this.tableAlias);
          }
        }

        // sqlExpression que NO usa @JOIN: - Incluir con prefijos corregidos pero solo si tenemos JOINs activos
        if (this.validJoins.length > 0) {
          // Tenemos JOINs - podemos usar el sqlExpression completo
          const tableName =
            this.entityMetadata.dbName || this.entityMetadata.key;
          const entityKey = this.entityMetadata.key;

          // Reemplazar tanto [TableName]. como TableName. (sin corchetes) con el alias
          const escapedTable = tableName.replace(/[.*+?^${}()|[\]\\]/g, '\\$&');
          const escapedEntity = entityKey.replace(/[.*+?^${}()|[\]\\]/g, '\\$&');
          const tableNameRegex1 = new RegExp(`\\[${escapedTable}\\]\\.`, 'gi');
          const tableNameRegex2 = new RegExp(`\\b${escapedTable}\\.`, 'gi');
          const entityKeyRegex1 = new RegExp(`\\[${escapedEntity}\\]\\.`, 'gi');
          const entityKeyRegex2 = new RegExp(`\\b${escapedEntity}\\.`, 'gi');

          cleanSql = cleanSql.replace(
            tableNameRegex1,
            `${this.wrapIdentifier(this.tableAlias)}.`
          );
          cleanSql = cleanSql.replace(
            tableNameRegex2,
            `${this.wrapIdentifier(this.tableAlias)}.`
          );
          cleanSql = cleanSql.replace(
            entityKeyRegex1,
            `${this.wrapIdentifier(this.tableAlias)}.`
          );
          cleanSql = cleanSql.replace(
            entityKeyRegex2,
            `${this.wrapIdentifier(this.tableAlias)}.`
          );

          // 🛑 Evitar duplicar "main."
          cleanSql = cleanSql.replace(/main\.main\./g, 'main.');
          cleanSql = cleanSql.replace(/\[main\]\.\[main\]\./g, 'main.');
          cleanSql = cleanSql.replace(/"main"\."main"\./g, '"main".');
          // TODO verificar si esto es necesario
          // cleanSql = cleanSql.replace(/\[\[/g, '[');
          // cleanSql = cleanSql.replace(/\]\]/g, ']');

          // Reemplazar referencias a columnas individuales con el alias si son de la tabla principal
          cleanSql = cleanSql.replace(/\[([^\]]+)\]/g, (match, columnName) => {
            const beforeMatch = cleanSql.substring(0, cleanSql.indexOf(match));
            if (beforeMatch.match(/\w+\s*\.\s*$/)) {
              return match;
            }
            // Buscar el campo por key o por dbName
            const mainField = this.entityMetadata.fields
              .toArray()
              .find((f: any) => {
                const dbName = (f.options as any)?.dbName || f.key;
                return f.key === columnName || dbName === columnName;
              });
            if (mainField) {
              // 🛑 Solo agregar prefijo si no está ya prefijado
              const wrappedColumn = this.col(this.tableAlias, columnName);
              if (!cleanSql.includes(wrappedColumn)) {
                return wrappedColumn;
              }
            }
            return this.wrapIdentifier(columnName);
          });
          return `(${this.normalizeSql(cleanSql)})`;
        } else {
          // No hay JOINs - ignorar este campo (sqlExpression sin JOINs activos)
          return null;
        }
      }
    }

    // AUTO-GENERAR para campos serverExpression con JOINs activos
    if (
      col.isServerExpression &&
      fieldOptions?.serverExpression &&
      this.validJoins.length > 0
    ) {
      const autoSql = this.tryGenerateSqlFromServerExpression(
        col.key,
        fieldOptions.serverExpression
      );

      if (autoSql) {
        return autoSql;
      }
    }

    // Campo normal
    const dbName = fieldOptions?.dbName || col.key;
    // Agregar prefijo de tabla si no lo tiene
    return dbName.includes('.')
      ? this.normalizeSql(dbName)
      : this.col(this.tableAlias, dbName);
  }

  /**
   * Intenta generar SQL automáticamente desde un serverExpression
   */
  private tryGenerateSqlFromServerExpression(
    fieldKey: string,
    serverExpression: Function
  ): string | null {
    try {
      const fnString = serverExpression.toString();
      const match = fnString.match(/\w+\.(\w+)\?\.(\w+)/);

      if (!match) return null;

      const [, relationKey, targetFieldKey] = match;

      const join = this.validJoins.find(
        (j) => j.joinAlias === `join_${relationKey}`
      );
      if (!join) return null;

      const targetField = join.relatedEntity.fields
        .toArray()
        .find((f) => f.key === targetFieldKey);
      if (!targetField) return null;

      const targetFieldOptions = targetField.options as any;

      // Si el campo target es un campo calculado (tiene sqlExpression o serverExpression),
      // NO intentar auto-generar SQL porque ya fue resuelto en el JOIN
      if (targetFieldOptions?.sqlExpression || targetField.isServerExpression) {
        return null;
      }

      const targetDbName = targetFieldOptions?.dbName || targetField.key;

      return this.col(join.joinAlias, targetDbName);
    } catch (error) {
      return null;
    }
  }

  private translateValue(col: any, val: any): any {
    // Usar el valueConverter del field si está disponible
    if (col.valueConverter && col.valueConverter.toDb) {
      return col.valueConverter.toDb(val);
    }
    return val;
  }

  /**
   * Convierte una expresión @JOIN:relation.field a un subquery SQL
   */
  private convertJoinToSubquery(
    sqlExpr: string,
    fieldKey: string,
    tableAlias: string
  ): string {
    const tableName = this.entityMetadata.dbName || this.entityMetadata.key;
    const mainTableAlias = tableAlias || tableName;

    // Buscar todas las ocurrencias de @JOIN:relation.field
    return sqlExpr.replace(
      /@JOIN:(\w+)\.(\w+)/g,
      (match, relationKey, fieldName) => {
        // Buscar el campo de relación (que tiene el FK ID)
        const fkField = this.entityMetadata.fields
          .toArray()
          .find((f) => f.key === `${relationKey}Id`);

        if (!fkField) {
          return 'NULL';
        }

        // Buscar la relación en la metadata (intenta primero en defaultIncluded, luego en todas las relaciones)
        const fieldOptions = (fkField as any).options;
        const relationInfo = (fkField as any)[fieldRelationInfo];

        if (!relationInfo) {
          return 'NULL';
        }

        const relatedEntity = relationInfo.toRepo?.metadata;
        if (!relatedEntity) {
          return 'NULL';
        }

        const relatedTableName = relatedEntity.dbName || relatedEntity.key;
        const relatedIdField = relatedEntity.idMetadata?.fields?.[0];
        const relatedIdDbName = relatedIdField
          ? (relatedIdField.options as any)?.dbName || relatedIdField.key
          : 'id';

        // Buscar el campo en la entidad relacionada
        const relatedField = relatedEntity.fields.toArray().find((f: any) => {
          const dbName = (f.options as any)?.dbName || f.key;
          return f.key === fieldName || dbName === fieldName;
        });

        if (!relatedField) {
          return 'NULL';
        }

        const relatedFieldDbName =
          (relatedField.options as any)?.dbName || relatedField.key;
        const fkFieldDbName = fieldOptions?.dbName || fkField.key;

        // Si la entidad relacionada tiene sqlExpression (es una subquery), usarla
        const relatedEntityOptions = (relatedEntity as any).options;
        if (relatedEntityOptions?.sqlExpression) {
          let relatedTableExpression =
            typeof relatedEntityOptions.sqlExpression === 'function'
              ? relatedEntityOptions.sqlExpression(relatedEntity)
              : relatedEntityOptions.sqlExpression;

          // Limpiar alias si tiene (con o sin "as", y también después de paréntesis)
          relatedTableExpression = relatedTableExpression
            .replace(/\s+as\s+\w+\s*$/i, '')
            .replace(/\)\s+([a-zA-Z_][a-zA-Z0-9_]*)\s*$/i, ')')
            .trim();

          return this.normalizeSql(
            `(SELECT ${this.wrapIdentifier(
              relatedFieldDbName
            )} FROM ${relatedTableExpression} WHERE ${this.wrapIdentifier(
              relatedIdDbName
            )} = ${this.col(mainTableAlias, fkFieldDbName)})`
          );
        }

        // Subquery normal
        return this.normalizeSql(
          `(SELECT ${this.wrapIdentifier(
            relatedFieldDbName
          )} FROM ${this.wrapTableName(
            relatedTableName
          )} WHERE ${this.wrapIdentifier(relatedIdDbName)} = ${this.col(
            mainTableAlias,
            fkFieldDbName
          )})`
        );
      }
    );
  }
}

export function createOptimizedProvider(
  baseProvider: SafeKnexDataProvider
): OptimizedDataProvider {
  return new OptimizedDataProvider(baseProvider);
}

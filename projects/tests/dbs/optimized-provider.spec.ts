import { describe, it, expect, beforeEach, beforeAll } from 'vitest'
import * as Knex from 'knex'
import { Entity, Fields, Relations, Remult, type Repository } from '../../core'
import type { ClassType } from '../../core/classType'
import { KnexSchemaBuilder } from '../../core/remult-knex'
import { SafeKnexDataProvider } from '../../core/src/mssql-safe-knex-provider'
import {
  OptimizedDataProvider,
  createOptimizedProvider,
} from '../../core/src/optimized-provider'

KnexSchemaBuilder.logToConsole = false

@Entity('opt_company')
class Company {
  @Fields.integer()
  id = 0
  @Fields.string()
  name = ''
  @Fields.string()
  city = ''
}

@Entity('opt_category')
class Category {
  @Fields.integer()
  id = 0
  @Fields.string()
  name = ''
  @Fields.string()
  description = ''
}

@Entity('opt_product')
class Product {
  @Fields.integer()
  id = 0
  @Fields.string()
  name = ''
  @Fields.number()
  price = 0

  @Fields.integer()
  categoryId = 0
  @Relations.toOne<Product, Category>(() => Category, 'categoryId')
  category?: Category
}

@Entity('opt_employee')
class Employee {
  @Fields.integer()
  id = 0
  @Fields.string()
  firstName = ''
  @Fields.string()
  lastName = ''

  @Fields.integer()
  companyId = 0
  @Relations.toOne<Employee, Company>(() => Company, 'companyId')
  company?: Company

  @Fields.integer({ allowNull: true })
  managerId: number | null = null
  @Relations.toOne<Employee, Employee>(() => Employee, 'managerId')
  manager?: Employee
}

@Entity('opt_order')
class Order {
  @Fields.integer()
  id = 0
  @Fields.string()
  orderNumber = ''
  @Fields.date()
  orderDate = new Date()

  @Fields.integer()
  customerId = 0
  @Relations.toOne<Order, Company>(() => Company, 'customerId')
  customer?: Company

  @Fields.string({ sqlExpression: () => '@JOIN:customer.name' })
  customerName = ''

  @Fields.string({ sqlExpression: () => '@JOIN:customer.city' })
  customerCity = ''
}

async function setupProvider(knex: Knex.Knex) {
  const base = new SafeKnexDataProvider(knex)
  const provider = createOptimizedProvider(base)
  const remult = new Remult(provider)
  return { provider, remult, knex, base }
}

async function createEntity<T>(
  knex: Knex.Knex,
  remult: Remult,
  base: SafeKnexDataProvider,
  entity: ClassType<T>,
): Promise<Repository<T>> {
  const repo = remult.repo(entity)
  await knex.schema.dropTableIfExists(repo.metadata.dbName!)
  await base.ensureSchema([repo.metadata])
  return repo
}

function runOptimizedProviderTests(createKnex: () => Knex.Knex) {
  let knex: Knex.Knex
  let provider: OptimizedDataProvider
  let remult: Remult
  let base: SafeKnexDataProvider

  beforeAll(async () => {
    knex = createKnex()
  }, 30000)

  beforeEach(async () => {
    const setup = await setupProvider(knex)
    provider = setup.provider
    remult = setup.remult
    base = setup.base
  })

  describe('Basic CRUD Operations', () => {
    it('should insert and find entities', async () => {
      const repo = await createEntity(knex, remult, base, Company)
      await repo.insert({ id: 1, name: 'Acme Corp', city: 'NYC' })
      await repo.insert({ id: 2, name: 'Tech Inc', city: 'SF' })

      const companies = await repo.find()
      expect(companies.length).toBe(2)
      expect(companies[0].name).toBe('Acme Corp')
    })

    it('should update entities', async () => {
      const repo = await createEntity(knex, remult, base, Company)
      await repo.insert({ id: 1, name: 'Old Name', city: 'NYC' })

      await repo.update(1, { name: 'New Name' })
      const updated = await repo.findId(1)
      expect(updated?.name).toBe('New Name')
    })

    it('should delete entities', async () => {
      const repo = await createEntity(knex, remult, base, Company)
      await repo.insert({ id: 1, name: 'ToDelete', city: 'NYC' })

      await repo.delete(1)
      expect(await repo.count()).toBe(0)
    })

    it('should count entities with filter', async () => {
      const repo = await createEntity(knex, remult, base, Company)
      await repo.insert([
        { id: 1, name: 'A', city: 'NYC' },
        { id: 2, name: 'B', city: 'NYC' },
        { id: 3, name: 'C', city: 'LA' },
      ])

      expect(await repo.count({ city: 'NYC' })).toBe(2)
    })
  })

  describe('toOne Relations with Automatic JOINs', () => {
    it('should load toOne relation', async () => {
      const categoryRepo = await createEntity(knex, remult, base, Category)
      const productRepo = await createEntity(knex, remult, base, Product)

      await categoryRepo.insert({
        id: 1,
        name: 'Electronics',
        description: 'Gadgets',
      })
      await productRepo.insert({ id: 1, name: 'Phone', price: 999, categoryId: 1 })

      const products = await productRepo.find({ include: { category: true } })
      expect(products.length).toBe(1)
      expect(products[0].category).toBeDefined()
      expect(products[0].category?.name).toBe('Electronics')
    })

    it('should handle null relations when FK references nonexistent row', async () => {
      const categoryRepo = await createEntity(knex, remult, base, Category)
      const productRepo = await createEntity(knex, remult, base, Product)

      await categoryRepo.insert({
        id: 1,
        name: 'Electronics',
        description: 'Gadgets',
      })
      await productRepo.insert({ id: 1, name: 'Orphan', price: 50, categoryId: 999 })

      const products = await productRepo.find({ include: { category: true } })
      expect(products.length).toBe(1)
      expect(products[0].category).toBeFalsy()
    })

    it('should filter by related entity id', async () => {
      const companyRepo = await createEntity(knex, remult, base, Company)
      const employeeRepo = await createEntity(knex, remult, base, Employee)

      await companyRepo.insert([
        { id: 1, name: 'Acme', city: 'NYC' },
        { id: 2, name: 'Tech', city: 'SF' },
      ])
      await employeeRepo.insert([
        { id: 1, firstName: 'John', lastName: 'Doe', companyId: 1 },
        { id: 2, firstName: 'Jane', lastName: 'Smith', companyId: 2 },
        { id: 3, firstName: 'Bob', lastName: 'Johnson', companyId: 1 },
      ])

      const acmeEmployees = await employeeRepo.find({
        where: { company: { $id: 1 } },
        include: { company: true },
      })
      expect(acmeEmployees.length).toBe(2)
      expect(acmeEmployees.every((e) => e.company?.name === 'Acme')).toBe(true)
    })
  })

  describe('@JOIN: sqlExpression Convention', () => {
    it('should resolve @JOIN: fields via JOIN', async () => {
      const companyRepo = await createEntity(knex, remult, base, Company)
      const orderRepo = await createEntity(knex, remult, base, Order)

      await companyRepo.insert({ id: 1, name: 'Customer One', city: 'Boston' })
      await orderRepo.insert({
        id: 1,
        orderNumber: 'ORD-001',
        orderDate: new Date('2024-01-15'),
        customerId: 1,
      })

      const orders = await orderRepo.find({ include: { customer: true } })
      expect(orders.length).toBe(1)
      expect(orders[0].customerName).toBe('Customer One')
      expect(orders[0].customerCity).toBe('Boston')
    })

    it('should filter by @JOIN: computed field', async () => {
      const companyRepo = await createEntity(knex, remult, base, Company)
      const orderRepo = await createEntity(knex, remult, base, Order)

      await companyRepo.insert([
        { id: 1, name: 'Alpha Corp', city: 'NYC' },
        { id: 2, name: 'Beta Inc', city: 'LA' },
      ])
      await orderRepo.insert([
        { id: 1, orderNumber: 'ORD-001', orderDate: new Date(), customerId: 1 },
        { id: 2, orderNumber: 'ORD-002', orderDate: new Date(), customerId: 2 },
        { id: 3, orderNumber: 'ORD-003', orderDate: new Date(), customerId: 1 },
      ])

      const nycOrders = await orderRepo.find({
        where: { customerCity: 'NYC' },
        include: { customer: true },
      })
      expect(nycOrders.length).toBe(2)
    })
  })

  describe('Self-referential Relations', () => {
    it('should handle self-referential toOne (manager)', async () => {
      const companyRepo = await createEntity(knex, remult, base, Company)
      const employeeRepo = await createEntity(knex, remult, base, Employee)

      await companyRepo.insert({ id: 1, name: 'Corp', city: 'NYC' })
      await employeeRepo.insert([
        { id: 1, firstName: 'Boss', lastName: 'Man', companyId: 1, managerId: null },
        { id: 2, firstName: 'Worker', lastName: 'Bee', companyId: 1, managerId: 1 },
      ])

      const employees = await employeeRepo.find({
        include: { manager: true, company: true },
        orderBy: { id: 'asc' },
      })

      expect(employees[0].manager).toBeNull()
      expect(employees[1].manager?.firstName).toBe('Boss')
    })
  })

  describe('Pagination and Sorting', () => {
    it('should paginate results', async () => {
      const repo = await createEntity(knex, remult, base, Company)
      await repo.insert([
        { id: 1, name: 'A', city: 'NYC' },
        { id: 2, name: 'B', city: 'NYC' },
        { id: 3, name: 'C', city: 'NYC' },
        { id: 4, name: 'D', city: 'NYC' },
        { id: 5, name: 'E', city: 'NYC' },
      ])

      const page1 = await repo.find({ limit: 2, page: 1, orderBy: { id: 'asc' } })
      const page2 = await repo.find({ limit: 2, page: 2, orderBy: { id: 'asc' } })

      expect(page1.map((c) => c.id)).toEqual([1, 2])
      expect(page2.map((c) => c.id)).toEqual([3, 4])
    })

    it('should sort results', async () => {
      const companyRepo = await createEntity(knex, remult, base, Company)
      const employeeRepo = await createEntity(knex, remult, base, Employee)

      await companyRepo.insert([
        { id: 1, name: 'Zebra Corp', city: 'NYC' },
        { id: 2, name: 'Alpha Inc', city: 'LA' },
      ])
      await employeeRepo.insert([
        { id: 1, firstName: 'John', lastName: 'Doe', companyId: 1 },
        { id: 2, firstName: 'Jane', lastName: 'Smith', companyId: 2 },
      ])

      const employees = await employeeRepo.find({
        orderBy: { id: 'asc' },
        include: { company: true },
      })
      expect(employees[0].firstName).toBe('John')
    })
  })

  describe('Complex Filters', () => {
    it('should handle $or filters', async () => {
      const repo = await createEntity(knex, remult, base, Company)
      await repo.insert([
        { id: 1, name: 'A', city: 'NYC' },
        { id: 2, name: 'B', city: 'LA' },
        { id: 3, name: 'C', city: 'SF' },
      ])

      const result = await repo.find({
        where: { $or: [{ city: 'NYC' }, { city: 'LA' }] },
      })
      expect(result.length).toBe(2)
    })

    it('should handle isIn filter', async () => {
      const repo = await createEntity(knex, remult, base, Company)
      await repo.insert([
        { id: 1, name: 'A', city: 'NYC' },
        { id: 2, name: 'B', city: 'LA' },
        { id: 3, name: 'C', city: 'SF' },
      ])

      const result = await repo.find({
        where: { id: [1, 3] },
      })
      expect(result.length).toBe(2)
      expect(result.map((c) => c.id).sort()).toEqual([1, 3])
    })

    it('should handle containsCaseInsensitive', async () => {
      const repo = await createEntity(knex, remult, base, Company)
      await repo.insert([
        { id: 1, name: 'Acme Corporation', city: 'NYC' },
        { id: 2, name: 'Tech Startup', city: 'LA' },
        { id: 3, name: 'Another ACME', city: 'SF' },
      ])

      const result = await repo.find({
        where: { name: { $contains: 'acme' } },
      })
      expect(result.length).toBe(2)
    })
  })

  describe('Provider Methods', () => {
    it('should expose transaction method', async () => {
      const repo = await createEntity(knex, remult, base, Company)

      await provider.transaction(async (txProvider) => {
        const txRemult = new Remult(txProvider)
        await txRemult.repo(Company).insert({ id: 1, name: 'In Transaction', city: 'TX' })
      })

      expect(await repo.count()).toBe(1)
    })
  })

  describe('Multiple Relations on Same Entity', () => {
    it('should handle employee with company and manager relations', async () => {
      const companyRepo = await createEntity(knex, remult, base, Company)
      const employeeRepo = await createEntity(knex, remult, base, Employee)

      await companyRepo.insert({ id: 1, name: 'Corp', city: 'NYC' })
      await employeeRepo.insert([
        { id: 1, firstName: 'CEO', lastName: 'Boss', companyId: 1, managerId: null },
        { id: 2, firstName: 'CTO', lastName: 'Tech', companyId: 1, managerId: 1 },
        { id: 3, firstName: 'Dev', lastName: 'Coder', companyId: 1, managerId: 2 },
      ])

      const employees = await employeeRepo.find({
        include: { company: true, manager: true },
        orderBy: { id: 'asc' },
      })

      expect(employees[0].company?.name).toBe('Corp')
      expect(employees[0].manager).toBeNull()
      expect(employees[1].manager?.firstName).toBe('CEO')
      expect(employees[2].manager?.firstName).toBe('CTO')
    })
  })

  describe('Edge Cases', () => {
    it('should return empty array when no rows match', async () => {
      const repo = await createEntity(knex, remult, base, Company)
      const result = await repo.find({ where: { id: 999 } })
      expect(result).toEqual([])
    })

    it('should handle null values in non-relation fields', async () => {
      const employeeRepo = await createEntity(knex, remult, base, Employee)
      const companyRepo = await createEntity(knex, remult, base, Company)

      await companyRepo.insert({ id: 1, name: 'Corp', city: 'NYC' })
      await employeeRepo.insert({
        id: 1,
        firstName: 'John',
        lastName: 'Doe',
        companyId: 1,
        managerId: null,
      })

      const employees = await employeeRepo.find({ include: { company: true } })
      expect(employees.length).toBe(1)
      expect(employees[0].managerId).toBeNull()
    })

    it('should handle special characters in string fields', async () => {
      const repo = await createEntity(knex, remult, base, Company)
      await repo.insert({ id: 1, name: "O'Brien & Co.", city: 'New York' })

      const companies = await repo.find()
      expect(companies[0].name).toBe("O'Brien & Co.")
    })

    it('should handle large result sets with relations', async () => {
      const companyRepo = await createEntity(knex, remult, base, Company)
      const employeeRepo = await createEntity(knex, remult, base, Employee)

      await companyRepo.insert({ id: 1, name: 'BigCorp', city: 'NYC' })
      const employees = Array.from({ length: 50 }, (_, i) => ({
        id: i + 1,
        firstName: `Employee${i + 1}`,
        lastName: `Last${i + 1}`,
        companyId: 1,
        managerId: null,
      }))
      await employeeRepo.insert(employees)

      const result = await employeeRepo.find({
        include: { company: true },
        orderBy: { id: 'asc' },
      })
      expect(result.length).toBe(50)
      expect(result.every((e) => e.company?.name === 'BigCorp')).toBe(true)
    })
  })

  describe('Count with JOINs', () => {
    it('should count correctly with filter on main entity', async () => {
      const companyRepo = await createEntity(knex, remult, base, Company)
      const employeeRepo = await createEntity(knex, remult, base, Employee)

      await companyRepo.insert([
        { id: 1, name: 'Corp1', city: 'NYC' },
        { id: 2, name: 'Corp2', city: 'LA' },
      ])
      await employeeRepo.insert([
        { id: 1, firstName: 'A', lastName: 'A', companyId: 1 },
        { id: 2, firstName: 'B', lastName: 'B', companyId: 1 },
        { id: 3, firstName: 'C', lastName: 'C', companyId: 2 },
      ])

      const count = await employeeRepo.count({ companyId: 1 })
      expect(count).toBe(2)
    })

    it('should count correctly with filter on related entity via $id', async () => {
      const companyRepo = await createEntity(knex, remult, base, Company)
      const employeeRepo = await createEntity(knex, remult, base, Employee)

      await companyRepo.insert([
        { id: 1, name: 'Corp1', city: 'NYC' },
        { id: 2, name: 'Corp2', city: 'LA' },
      ])
      await employeeRepo.insert([
        { id: 1, firstName: 'A', lastName: 'A', companyId: 1 },
        { id: 2, firstName: 'B', lastName: 'B', companyId: 1 },
        { id: 3, firstName: 'C', lastName: 'C', companyId: 2 },
      ])

      const count = await employeeRepo.count({ company: { $id: 1 } })
      expect(count).toBe(2)
    })
  })

  describe('Comparison Operators', () => {
    it('should handle $gt filter', async () => {
      const repo = await createEntity(knex, remult, base, Product)
      const categoryRepo = await createEntity(knex, remult, base, Category)

      await categoryRepo.insert({ id: 1, name: 'Electronics', description: '' })
      await repo.insert([
        { id: 1, name: 'Cheap', price: 10, categoryId: 1 },
        { id: 2, name: 'Medium', price: 50, categoryId: 1 },
        { id: 3, name: 'Expensive', price: 100, categoryId: 1 },
      ])

      const result = await repo.find({ where: { price: { $gt: 30 } } })
      expect(result.length).toBe(2)
    })

    it('should handle $lt filter', async () => {
      const repo = await createEntity(knex, remult, base, Product)
      const categoryRepo = await createEntity(knex, remult, base, Category)

      await categoryRepo.insert({ id: 1, name: 'Electronics', description: '' })
      await repo.insert([
        { id: 1, name: 'Cheap', price: 10, categoryId: 1 },
        { id: 2, name: 'Medium', price: 50, categoryId: 1 },
        { id: 3, name: 'Expensive', price: 100, categoryId: 1 },
      ])

      const result = await repo.find({ where: { price: { $lt: 50 } } })
      expect(result.length).toBe(1)
      expect(result[0].name).toBe('Cheap')
    })

    it('should handle $gte and $lte filters', async () => {
      const repo = await createEntity(knex, remult, base, Product)
      const categoryRepo = await createEntity(knex, remult, base, Category)

      await categoryRepo.insert({ id: 1, name: 'Electronics', description: '' })
      await repo.insert([
        { id: 1, name: 'Cheap', price: 10, categoryId: 1 },
        { id: 2, name: 'Medium', price: 50, categoryId: 1 },
        { id: 3, name: 'Expensive', price: 100, categoryId: 1 },
      ])

      const result = await repo.find({
        where: { price: { $gte: 10, $lte: 50 } },
        orderBy: { price: 'asc' },
      })
      expect(result.length).toBe(2)
      expect(result[0].name).toBe('Cheap')
      expect(result[1].name).toBe('Medium')
    })

    it('should handle $ne filter', async () => {
      const repo = await createEntity(knex, remult, base, Company)
      await repo.insert([
        { id: 1, name: 'A', city: 'NYC' },
        { id: 2, name: 'B', city: 'LA' },
        { id: 3, name: 'C', city: 'NYC' },
      ])

      const result = await repo.find({ where: { city: { $ne: 'NYC' } } })
      expect(result.length).toBe(1)
      expect(result[0].city).toBe('LA')
    })
  })

  describe('Multiple Filters Combined', () => {
    it('should handle AND filters correctly', async () => {
      const repo = await createEntity(knex, remult, base, Company)
      await repo.insert([
        { id: 1, name: 'BigCorp', city: 'NYC' },
        { id: 2, name: 'SmallCorp', city: 'NYC' },
        { id: 3, name: 'BigCorp', city: 'LA' },
      ])

      const result = await repo.find({
        where: { name: 'BigCorp', city: 'NYC' },
      })
      expect(result.length).toBe(1)
      expect(result[0].id).toBe(1)
    })

    it('should handle nested $or with $and', async () => {
      const repo = await createEntity(knex, remult, base, Company)
      await repo.insert([
        { id: 1, name: 'A', city: 'NYC' },
        { id: 2, name: 'B', city: 'LA' },
        { id: 3, name: 'C', city: 'SF' },
        { id: 4, name: 'D', city: 'NYC' },
      ])

      const result = await repo.find({
        where: { $or: [{ city: 'NYC' }, { name: 'B' }] },
        orderBy: { id: 'asc' },
      })
      expect(result.length).toBe(3)
      expect(result.map((c) => c.id)).toEqual([1, 2, 4])
    })
  })

  describe('Date Field Handling', () => {
    it('should correctly save and retrieve date fields', async () => {
      const companyRepo = await createEntity(knex, remult, base, Company)
      const orderRepo = await createEntity(knex, remult, base, Order)

      await companyRepo.insert({ id: 1, name: 'Customer', city: 'NYC' })
      const testDate = new Date('2024-06-15T12:00:00.000Z')
      await orderRepo.insert({
        id: 1,
        orderNumber: 'ORD-001',
        orderDate: testDate,
        customerId: 1,
      })

      const orders = await orderRepo.find()
      expect(orders[0].orderDate).toBeInstanceOf(Date)
      expect(orders[0].orderDate.toISOString()).toBe(testDate.toISOString())
    })

    it('should filter by date', async () => {
      const companyRepo = await createEntity(knex, remult, base, Company)
      const orderRepo = await createEntity(knex, remult, base, Order)

      await companyRepo.insert({ id: 1, name: 'Customer', city: 'NYC' })
      await orderRepo.insert([
        { id: 1, orderNumber: 'ORD-001', orderDate: new Date('2024-01-15'), customerId: 1 },
        { id: 2, orderNumber: 'ORD-002', orderDate: new Date('2024-06-15'), customerId: 1 },
        { id: 3, orderNumber: 'ORD-003', orderDate: new Date('2024-12-15'), customerId: 1 },
      ])

      const result = await orderRepo.find({
        where: { orderDate: { $gte: new Date('2024-06-01') } },
        orderBy: { orderDate: 'asc' },
      })
      expect(result.length).toBe(2)
    })
  })
}

const postgresConnection =
  'postgres://postgres:drVURROdPnlt8RikyaYpCKTMl6ja4QLS5wRqCaAU9HD4QgTgbL50tTR5Y3K1nOdn@192.168.8.150:5836/postgres'

describe('OptimizedDataProvider - PostgreSQL', () => {
  runOptimizedProviderTests(() =>
    Knex.default({
      client: 'pg',
      connection: postgresConnection,
    }),
  )
})

describe('OptimizedDataProvider - MSSQL', () => {
  runOptimizedProviderTests(() =>
    Knex.default({
      client: 'mssql',
      connection: {
        server: '46.4.187.155',
        database: 'test3',
        user: 'sa',
        password: 'kjsD2387mad',
        options: {
          enableArithAbort: true,
          encrypt: false,
        },
      },
    }),
  )
})

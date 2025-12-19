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
      await productRepo.insert({
        id: 1,
        name: 'Phone',
        price: 999,
        categoryId: 1,
      })

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
      await productRepo.insert({
        id: 1,
        name: 'Orphan',
        price: 50,
        categoryId: 999,
      })

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
        {
          id: 1,
          firstName: 'Boss',
          lastName: 'Man',
          companyId: 1,
          managerId: null,
        },
        {
          id: 2,
          firstName: 'Worker',
          lastName: 'Bee',
          companyId: 1,
          managerId: 1,
        },
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

      const page1 = await repo.find({
        limit: 2,
        page: 1,
        orderBy: { id: 'asc' },
      })
      const page2 = await repo.find({
        limit: 2,
        page: 2,
        orderBy: { id: 'asc' },
      })

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
        await txRemult
          .repo(Company)
          .insert({ id: 1, name: 'In Transaction', city: 'TX' })
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
        {
          id: 1,
          firstName: 'CEO',
          lastName: 'Boss',
          companyId: 1,
          managerId: null,
        },
        {
          id: 2,
          firstName: 'CTO',
          lastName: 'Tech',
          companyId: 1,
          managerId: 1,
        },
        {
          id: 3,
          firstName: 'Dev',
          lastName: 'Coder',
          companyId: 1,
          managerId: 2,
        },
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
        {
          id: 1,
          orderNumber: 'ORD-001',
          orderDate: new Date('2024-01-15'),
          customerId: 1,
        },
        {
          id: 2,
          orderNumber: 'ORD-002',
          orderDate: new Date('2024-06-15'),
          customerId: 1,
        },
        {
          id: 3,
          orderNumber: 'ORD-003',
          orderDate: new Date('2024-12-15'),
          customerId: 1,
        },
      ])

      const result = await orderRepo.find({
        where: { orderDate: { $gte: new Date('2024-06-01') } },
        orderBy: { orderDate: 'asc' },
      })
      expect(result.length).toBe(2)
    })
  })
}

@Entity('opt_country')
class Country {
  @Fields.integer()
  id = 0
  @Fields.string()
  name = ''
  @Fields.string()
  continent = ''
}

@Entity('opt_region')
class Region {
  @Fields.integer()
  id = 0
  @Fields.string()
  name = ''

  @Fields.integer()
  countryId = 0
  @Relations.toOne<Region, Country>(() => Country, 'countryId')
  country?: Country
}

@Entity('opt_city')
class City {
  @Fields.integer()
  id = 0
  @Fields.string()
  name = ''
  @Fields.integer()
  population = 0

  @Fields.integer()
  regionId = 0
  @Relations.toOne<City, Region>(() => Region, 'regionId')
  region?: Region

  @Fields.string({ sqlExpression: () => '@JOIN:region.name' })
  regionName = ''
}

@Entity('opt_invoice')
class Invoice {
  @Fields.integer()
  id = 0
  @Fields.string()
  invoiceNumber = ''
  @Fields.number()
  amount = 0

  @Fields.integer()
  customerId = 0
  @Relations.toOne<Invoice, Company>(() => Company, 'customerId')
  customer?: Company

  @Fields.string({ sqlExpression: () => '@JOIN:customer.name' })
  customerName = ''

  @Fields.string({ sqlExpression: () => '@JOIN:customer.city' })
  customerCity = ''
}

@Entity('opt_task')
class Task {
  @Fields.integer()
  id = 0
  @Fields.string()
  title = ''
  @Fields.string()
  priority = ''
  @Fields.date()
  dueDate = new Date()

  @Fields.integer()
  assigneeId = 0
  @Relations.toOne<Task, Employee>(() => Employee, 'assigneeId')
  assignee?: Employee

  @Fields.string({ sqlExpression: () => '@JOIN:assignee.firstName' })
  assigneeName = ''
}

function runAdvancedOptimizedProviderTests(createKnex: () => Knex.Knex) {
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

  describe('Deeply Nested Relations (A→B→C)', () => {
    it('should load 3-level nested relations', async () => {
      const countryRepo = await createEntity(knex, remult, base, Country)
      const regionRepo = await createEntity(knex, remult, base, Region)
      const cityRepo = await createEntity(knex, remult, base, City)

      await countryRepo.insert({
        id: 1,
        name: 'USA',
        continent: 'North America',
      })
      await regionRepo.insert({ id: 1, name: 'California', countryId: 1 })
      await cityRepo.insert({
        id: 1,
        name: 'Los Angeles',
        population: 4000000,
        regionId: 1,
      })

      const cities = await cityRepo.find({ include: { region: true } })
      expect(cities.length).toBe(1)
      expect(cities[0].region?.name).toBe('California')
      expect(cities[0].regionName).toBe('California')
    })

    it('should handle null in nested chain', async () => {
      const countryRepo = await createEntity(knex, remult, base, Country)
      const regionRepo = await createEntity(knex, remult, base, Region)
      const cityRepo = await createEntity(knex, remult, base, City)

      await countryRepo.insert({
        id: 1,
        name: 'USA',
        continent: 'North America',
      })
      await regionRepo.insert({ id: 1, name: 'California', countryId: 1 })
      await cityRepo.insert({
        id: 1,
        name: 'Orphan City',
        population: 100,
        regionId: 999,
      })

      const cities = await cityRepo.find({ include: { region: true } })
      expect(cities.length).toBe(1)
      expect(cities[0].region).toBeFalsy()
    })

    it('should filter by nested relation field', async () => {
      const countryRepo = await createEntity(knex, remult, base, Country)
      const regionRepo = await createEntity(knex, remult, base, Region)
      const cityRepo = await createEntity(knex, remult, base, City)

      await countryRepo.insert([
        { id: 1, name: 'USA', continent: 'North America' },
        { id: 2, name: 'Canada', continent: 'North America' },
      ])
      await regionRepo.insert([
        { id: 1, name: 'California', countryId: 1 },
        { id: 2, name: 'Ontario', countryId: 2 },
      ])
      await cityRepo.insert([
        { id: 1, name: 'Los Angeles', population: 4000000, regionId: 1 },
        { id: 2, name: 'Toronto', population: 2700000, regionId: 2 },
      ])

      // Filter by region via $id
      const californiaCities = await cityRepo.find({
        where: { region: { $id: 1 } },
        include: { region: true },
      })
      expect(californiaCities.length).toBe(1)
      expect(californiaCities[0].name).toBe('Los Angeles')
    })
  })

  describe('Multiple @JOIN Fields from Same Relation', () => {
    it('should resolve multiple @JOIN fields from same relation', async () => {
      const companyRepo = await createEntity(knex, remult, base, Company)
      const invoiceRepo = await createEntity(knex, remult, base, Invoice)

      await companyRepo.insert({ id: 1, name: 'Acme Corp', city: 'NYC' })
      await invoiceRepo.insert({
        id: 1,
        invoiceNumber: 'INV-001',
        amount: 1000,
        customerId: 1,
      })

      const invoices = await invoiceRepo.find({ include: { customer: true } })
      expect(invoices.length).toBe(1)
      expect(invoices[0].customerName).toBe('Acme Corp')
      expect(invoices[0].customerCity).toBe('NYC')
      expect(invoices[0].customer?.name).toBe('Acme Corp')
    })

    it('should handle null for multiple @JOIN fields', async () => {
      const companyRepo = await createEntity(knex, remult, base, Company)
      const invoiceRepo = await createEntity(knex, remult, base, Invoice)

      await companyRepo.insert({ id: 1, name: 'Acme Corp', city: 'NYC' })
      // Invoice with non-existent customer
      await invoiceRepo.insert({
        id: 1,
        invoiceNumber: 'INV-001',
        amount: 500,
        customerId: 999,
      })

      const invoices = await invoiceRepo.find({ include: { customer: true } })
      expect(invoices.length).toBe(1)
      expect(invoices[0].customer).toBeFalsy()
    })
  })

  describe('Sorting by @JOIN Computed Field', () => {
    it('should sort by @JOIN computed field ascending', async () => {
      const companyRepo = await createEntity(knex, remult, base, Company)
      const employeeRepo = await createEntity(knex, remult, base, Employee)
      const taskRepo = await createEntity(knex, remult, base, Task)

      await companyRepo.insert({ id: 1, name: 'Corp', city: 'NYC' })
      await employeeRepo.insert([
        { id: 1, firstName: 'Zoe', lastName: 'Smith', companyId: 1 },
        { id: 2, firstName: 'Alice', lastName: 'Johnson', companyId: 1 },
        { id: 3, firstName: 'Mike', lastName: 'Brown', companyId: 1 },
      ])
      await taskRepo.insert([
        {
          id: 1,
          title: 'Task A',
          priority: 'high',
          assigneeId: 1,
          dueDate: new Date(),
        },
        {
          id: 2,
          title: 'Task B',
          priority: 'low',
          assigneeId: 2,
          dueDate: new Date(),
        },
        {
          id: 3,
          title: 'Task C',
          priority: 'medium',
          assigneeId: 3,
          dueDate: new Date(),
        },
      ])

      const tasks = await taskRepo.find({
        orderBy: { assigneeName: 'asc' },
        include: { assignee: true },
      })

      expect(tasks[0].assigneeName).toBe('Alice')
      expect(tasks[1].assigneeName).toBe('Mike')
      expect(tasks[2].assigneeName).toBe('Zoe')
    })

    it('should sort by @JOIN computed field descending', async () => {
      const companyRepo = await createEntity(knex, remult, base, Company)
      const employeeRepo = await createEntity(knex, remult, base, Employee)
      const taskRepo = await createEntity(knex, remult, base, Task)

      await companyRepo.insert({ id: 1, name: 'Corp', city: 'NYC' })
      await employeeRepo.insert([
        { id: 1, firstName: 'Zoe', lastName: 'Smith', companyId: 1 },
        { id: 2, firstName: 'Alice', lastName: 'Johnson', companyId: 1 },
      ])
      await taskRepo.insert([
        {
          id: 1,
          title: 'Task A',
          priority: 'high',
          assigneeId: 1,
          dueDate: new Date(),
        },
        {
          id: 2,
          title: 'Task B',
          priority: 'low',
          assigneeId: 2,
          dueDate: new Date(),
        },
      ])

      const tasks = await taskRepo.find({
        orderBy: { assigneeName: 'desc' },
        include: { assignee: true },
      })

      expect(tasks[0].assigneeName).toBe('Zoe')
      expect(tasks[1].assigneeName).toBe('Alice')
    })
  })

  describe('Self-Referential Chain (manager→manager→manager)', () => {
    it('should handle 3-level manager hierarchy', async () => {
      const companyRepo = await createEntity(knex, remult, base, Company)
      const employeeRepo = await createEntity(knex, remult, base, Employee)

      await companyRepo.insert({ id: 1, name: 'Corp', city: 'NYC' })
      await employeeRepo.insert([
        {
          id: 1,
          firstName: 'CEO',
          lastName: 'Boss',
          companyId: 1,
          managerId: null,
        },
        {
          id: 2,
          firstName: 'VP',
          lastName: 'Middle',
          companyId: 1,
          managerId: 1,
        },
        {
          id: 3,
          firstName: 'Manager',
          lastName: 'Low',
          companyId: 1,
          managerId: 2,
        },
        {
          id: 4,
          firstName: 'Worker',
          lastName: 'Bee',
          companyId: 1,
          managerId: 3,
        },
      ])

      const employees = await employeeRepo.find({
        include: { manager: true },
        orderBy: { id: 'asc' },
      })

      expect(employees[0].manager).toBeNull() // CEO has no manager
      expect(employees[1].manager?.firstName).toBe('CEO')
      expect(employees[2].manager?.firstName).toBe('VP')
      expect(employees[3].manager?.firstName).toBe('Manager')
    })

    it('should filter self-referential by manager id', async () => {
      const companyRepo = await createEntity(knex, remult, base, Company)
      const employeeRepo = await createEntity(knex, remult, base, Employee)

      await companyRepo.insert({ id: 1, name: 'Corp', city: 'NYC' })
      await employeeRepo.insert([
        {
          id: 1,
          firstName: 'CEO',
          lastName: 'Boss',
          companyId: 1,
          managerId: null,
        },
        { id: 2, firstName: 'VP1', lastName: 'A', companyId: 1, managerId: 1 },
        { id: 3, firstName: 'VP2', lastName: 'B', companyId: 1, managerId: 1 },
        {
          id: 4,
          firstName: 'Worker',
          lastName: 'C',
          companyId: 1,
          managerId: 2,
        },
      ])

      // Find direct reports to CEO
      const directReports = await employeeRepo.find({
        where: { manager: { $id: 1 } },
        include: { manager: true },
      })

      expect(directReports.length).toBe(2)
      expect(directReports.every((e) => e.manager?.firstName === 'CEO')).toBe(
        true,
      )
    })
  })

  describe('Complex WHERE with $or/$and on @JOIN Fields', () => {
    it('should filter with $or on @JOIN field', async () => {
      const companyRepo = await createEntity(knex, remult, base, Company)
      const orderRepo = await createEntity(knex, remult, base, Order)

      await companyRepo.insert([
        { id: 1, name: 'Alpha Corp', city: 'NYC' },
        { id: 2, name: 'Beta Inc', city: 'LA' },
        { id: 3, name: 'Gamma LLC', city: 'Chicago' },
      ])
      await orderRepo.insert([
        { id: 1, orderNumber: 'ORD-001', orderDate: new Date(), customerId: 1 },
        { id: 2, orderNumber: 'ORD-002', orderDate: new Date(), customerId: 2 },
        { id: 3, orderNumber: 'ORD-003', orderDate: new Date(), customerId: 3 },
      ])

      const orders = await orderRepo.find({
        where: { $or: [{ customerCity: 'NYC' }, { customerCity: 'LA' }] },
        include: { customer: true },
      })

      expect(orders.length).toBe(2)
    })

    it('should filter with combined $and and relation filter', async () => {
      const companyRepo = await createEntity(knex, remult, base, Company)
      const orderRepo = await createEntity(knex, remult, base, Order)

      await companyRepo.insert([
        { id: 1, name: 'Alpha Corp', city: 'NYC' },
        { id: 2, name: 'Beta Inc', city: 'NYC' },
      ])
      await orderRepo.insert([
        {
          id: 1,
          orderNumber: 'ORD-001',
          orderDate: new Date('2024-01-01'),
          customerId: 1,
        },
        {
          id: 2,
          orderNumber: 'ORD-002',
          orderDate: new Date('2024-06-01'),
          customerId: 1,
        },
        {
          id: 3,
          orderNumber: 'ORD-003',
          orderDate: new Date('2024-01-01'),
          customerId: 2,
        },
      ])

      // Filter by customerCity AND customerId
      const orders = await orderRepo.find({
        where: {
          customerCity: 'NYC',
          customer: { $id: 1 },
        },
        include: { customer: true },
      })

      expect(orders.length).toBe(2)
      expect(orders.every((o) => o.customer?.name === 'Alpha Corp')).toBe(true)
    })
  })

  describe('Transaction Rollback Scenarios', () => {
    it('should rollback on error', async () => {
      const companyRepo = await createEntity(knex, remult, base, Company)

      // Insert initial data
      await companyRepo.insert({ id: 1, name: 'Initial', city: 'NYC' })
      expect(await companyRepo.count()).toBe(1)

      // Try transaction that will fail
      try {
        await provider.transaction(async (txProvider) => {
          const txRemult = new Remult(txProvider)
          await txRemult
            .repo(Company)
            .insert({ id: 2, name: 'In Transaction', city: 'TX' })

          // Verify it exists within transaction
          const count = await txRemult.repo(Company).count()
          expect(count).toBe(2)

          // Force error to trigger rollback
          throw new Error('Forced rollback')
        })
      } catch (e) {
        // Expected error
      }

      // Verify rollback happened
      expect(await companyRepo.count()).toBe(1)
    })

    it('should commit successful transaction', async () => {
      const companyRepo = await createEntity(knex, remult, base, Company)

      await provider.transaction(async (txProvider) => {
        const txRemult = new Remult(txProvider)
        await txRemult
          .repo(Company)
          .insert({ id: 1, name: 'Committed', city: 'TX' })
      })

      expect(await companyRepo.count()).toBe(1)
      expect((await companyRepo.findFirst())?.name).toBe('Committed')
    })
  })

  describe('Update/Delete with Loaded Relations', () => {
    it('should update entity after loading with relations', async () => {
      const companyRepo = await createEntity(knex, remult, base, Company)
      const employeeRepo = await createEntity(knex, remult, base, Employee)

      await companyRepo.insert({ id: 1, name: 'Corp', city: 'NYC' })
      await employeeRepo.insert({
        id: 1,
        firstName: 'John',
        lastName: 'Doe',
        companyId: 1,
      })

      // Load with relation
      const employees = await employeeRepo.find({ include: { company: true } })
      expect(employees[0].company?.name).toBe('Corp')

      // Update the employee
      await employeeRepo.update(1, { firstName: 'Jane' })

      // Verify update worked
      const updated = await employeeRepo.findId(1)
      expect(updated?.firstName).toBe('Jane')
    })

    it('should delete entity after loading with relations', async () => {
      const companyRepo = await createEntity(knex, remult, base, Company)
      const employeeRepo = await createEntity(knex, remult, base, Employee)

      await companyRepo.insert({ id: 1, name: 'Corp', city: 'NYC' })
      await employeeRepo.insert([
        { id: 1, firstName: 'John', lastName: 'Doe', companyId: 1 },
        { id: 2, firstName: 'Jane', lastName: 'Smith', companyId: 1 },
      ])

      // Load with relation
      const employees = await employeeRepo.find({ include: { company: true } })
      expect(employees.length).toBe(2)

      // Delete one
      await employeeRepo.delete(1)

      // Verify deletion
      expect(await employeeRepo.count()).toBe(1)
    })
  })

  describe('Null Handling in Nested Relations', () => {
    it('should handle all nulls in chain gracefully', async () => {
      const companyRepo = await createEntity(knex, remult, base, Company)
      const employeeRepo = await createEntity(knex, remult, base, Employee)

      await companyRepo.insert({ id: 1, name: 'Corp', city: 'NYC' })
      // Employee with null managerId and valid company
      await employeeRepo.insert({
        id: 1,
        firstName: 'Lonely',
        lastName: 'Worker',
        companyId: 1,
        managerId: null,
      })

      const employees = await employeeRepo.find({
        include: { company: true, manager: true },
      })

      expect(employees[0].company?.name).toBe('Corp')
      expect(employees[0].manager).toBeNull()
    })

    it('should count correctly with null relations', async () => {
      const companyRepo = await createEntity(knex, remult, base, Company)
      const employeeRepo = await createEntity(knex, remult, base, Employee)

      await companyRepo.insert({ id: 1, name: 'Corp', city: 'NYC' })
      await employeeRepo.insert([
        {
          id: 1,
          firstName: 'With Manager',
          lastName: 'A',
          companyId: 1,
          managerId: null,
        },
        {
          id: 2,
          firstName: 'Reports To 1',
          lastName: 'B',
          companyId: 1,
          managerId: 1,
        },
        {
          id: 3,
          firstName: 'Also Reports',
          lastName: 'C',
          companyId: 1,
          managerId: 1,
        },
      ])

      // Count those with manager
      const withManager = await employeeRepo.count({ manager: { $id: 1 } })
      expect(withManager).toBe(2)
    })
  })

  describe('Edge Cases - Empty Results and Boundaries', () => {
    it('should handle query returning zero results with relations', async () => {
      const companyRepo = await createEntity(knex, remult, base, Company)
      const employeeRepo = await createEntity(knex, remult, base, Employee)

      await companyRepo.insert({ id: 1, name: 'Corp', city: 'NYC' })
      // No employees

      const employees = await employeeRepo.find({ include: { company: true } })
      expect(employees).toEqual([])
    })

    it('should handle findId with relations when not found', async () => {
      const companyRepo = await createEntity(knex, remult, base, Company)
      const employeeRepo = await createEntity(knex, remult, base, Employee)

      await companyRepo.insert({ id: 1, name: 'Corp', city: 'NYC' })

      const employee = await employeeRepo.findId(999)
      expect(employee).toBeUndefined()
    })

    it('should handle pagination at boundaries', async () => {
      const companyRepo = await createEntity(knex, remult, base, Company)

      await companyRepo.insert([
        { id: 1, name: 'A', city: 'NYC' },
        { id: 2, name: 'B', city: 'NYC' },
        { id: 3, name: 'C', city: 'NYC' },
      ])

      // Request page beyond data
      const page10 = await companyRepo.find({
        limit: 2,
        page: 10,
        orderBy: { id: 'asc' },
      })
      expect(page10).toEqual([])

      // Request exactly at boundary
      const page2 = await companyRepo.find({
        limit: 2,
        page: 2,
        orderBy: { id: 'asc' },
      })
      expect(page2.length).toBe(1)
      expect(page2[0].id).toBe(3)
    })
  })
}

@Entity('opt_continent')
class Continent {
  @Fields.integer()
  id = 0
  @Fields.string()
  name = ''
}

@Entity('opt_country2')
class Country2 {
  @Fields.integer()
  id = 0
  @Fields.string()
  name = ''
  @Fields.string()
  code = ''

  @Fields.integer()
  continentId = 0
  @Relations.toOne<Country2, Continent>(() => Continent, 'continentId')
  continent?: Continent

  @Fields.string({ sqlExpression: () => '@JOIN:continent.name' })
  continentName = ''
}

@Entity('opt_region2')
class Region2 {
  @Fields.integer()
  id = 0
  @Fields.string()
  name = ''

  @Fields.integer()
  countryId = 0
  @Relations.toOne<Region2, Country2>(() => Country2, 'countryId')
  country?: Country2

  @Fields.string({ sqlExpression: () => '@JOIN:country.name' })
  countryName = ''

  @Fields.string({ sqlExpression: () => '@JOIN:country.code' })
  countryCode = ''
}

@Entity('opt_city2')
class City2 {
  @Fields.integer()
  id = 0
  @Fields.string()
  name = ''
  @Fields.integer()
  population = 0

  @Fields.integer()
  regionId = 0
  @Relations.toOne<City2, Region2>(() => Region2, 'regionId')
  region?: Region2

  @Fields.string({ sqlExpression: () => '@JOIN:region.name' })
  regionName = ''

  @Fields.string({ sqlExpression: () => '@JOIN:region.countryName' })
  countryName = ''
}

@Entity('opt_district')
class District {
  @Fields.integer()
  id = 0
  @Fields.string()
  name = ''
  @Fields.integer()
  area = 0

  @Fields.integer()
  cityId = 0
  @Relations.toOne<District, City2>(() => City2, 'cityId')
  city?: City2

  @Fields.string({ sqlExpression: () => '@JOIN:city.name' })
  cityName = ''

  @Fields.string({ sqlExpression: () => '@JOIN:city.regionName' })
  regionName = ''
}

@Entity('opt_unicode')
class UnicodeEntity {
  @Fields.integer()
  id = 0
  @Fields.string()
  name = ''
  @Fields.string()
  description = ''
}

@Entity('opt_unicode_ref')
class UnicodeRefEntity {
  @Fields.integer()
  id = 0
  @Fields.string()
  title = ''

  @Fields.integer()
  unicodeId = 0
  @Relations.toOne<UnicodeRefEntity, UnicodeEntity>(() => UnicodeEntity, 'unicodeId')
  unicode?: UnicodeEntity

  @Fields.string({ sqlExpression: () => '@JOIN:unicode.name' })
  unicodeName = ''

  @Fields.string({ sqlExpression: () => '@JOIN:unicode.description' })
  unicodeDescription = ''
}

@Entity('opt_address')
class Address {
  @Fields.integer()
  id = 0
  @Fields.string()
  street = ''
  @Fields.string()
  city = ''
  @Fields.string()
  country = ''
}

@Entity('opt_person')
class Person {
  @Fields.integer()
  id = 0
  @Fields.string()
  name = ''

  @Fields.integer()
  homeAddressId = 0
  @Relations.toOne<Person, Address>(() => Address, 'homeAddressId')
  homeAddress?: Address

  @Fields.integer()
  workAddressId = 0
  @Relations.toOne<Person, Address>(() => Address, 'workAddressId')
  workAddress?: Address

  @Fields.integer({ allowNull: true })
  billingAddressId: number | null = null
  @Relations.toOne<Person, Address>(() => Address, 'billingAddressId')
  billingAddress?: Address

  @Fields.string({ sqlExpression: () => '@JOIN:homeAddress.city' })
  homeCity = ''

  @Fields.string({ sqlExpression: () => '@JOIN:workAddress.city' })
  workCity = ''
}

@Entity('opt_org_unit')
class OrgUnit {
  @Fields.integer()
  id = 0
  @Fields.string()
  name = ''
  @Fields.string()
  code = ''
  @Fields.integer()
  level = 0

  @Fields.integer({ allowNull: true })
  parentId: number | null = null
  @Relations.toOne<OrgUnit, OrgUnit>(() => OrgUnit, 'parentId')
  parent?: OrgUnit

  @Fields.string({ sqlExpression: () => '@JOIN:parent.name' })
  parentName = ''

  @Fields.string({ sqlExpression: () => '@JOIN:parent.code' })
  parentCode = ''
}

@Entity('opt_sale')
class Sale {
  @Fields.integer()
  id = 0
  @Fields.number()
  amount = 0
  @Fields.date()
  saleDate = new Date()
  @Fields.string()
  status = ''

  @Fields.integer()
  customerId = 0
  @Relations.toOne<Sale, Company>(() => Company, 'customerId')
  customer?: Company

  @Fields.integer()
  salesRepId = 0
  @Relations.toOne<Sale, Employee>(() => Employee, 'salesRepId')
  salesRep?: Employee

  @Fields.string({ sqlExpression: () => '@JOIN:customer.name' })
  customerName = ''

  @Fields.string({ sqlExpression: () => '@JOIN:customer.city' })
  customerCity = ''

  @Fields.string({ sqlExpression: () => '@JOIN:salesRep.firstName' })
  salesRepFirstName = ''
}

function runExtremeEdgeCaseTests(createKnex: () => Knex.Knex) {
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

  describe('4+ Level Deep Nesting', () => {
    it('should handle 4-level deep relation chain with nested @JOIN', async () => {
      const continentRepo = await createEntity(knex, remult, base, Continent)
      const countryRepo = await createEntity(knex, remult, base, Country2)
      const regionRepo = await createEntity(knex, remult, base, Region2)
      const cityRepo = await createEntity(knex, remult, base, City2)
      const districtRepo = await createEntity(knex, remult, base, District)

      await continentRepo.insert({ id: 1, name: 'Europe' })
      await countryRepo.insert({ id: 1, name: 'Germany', code: 'DE', continentId: 1 })
      await regionRepo.insert({ id: 1, name: 'Bavaria', countryId: 1 })
      await cityRepo.insert({ id: 1, name: 'Munich', population: 1500000, regionId: 1 })
      await districtRepo.insert({ id: 1, name: 'Schwabing', area: 15, cityId: 1 })

      const cities = await cityRepo.find({ include: { region: true } })
      expect(cities.length).toBe(1)
      expect(cities[0].regionName).toBe('Bavaria')
      expect(cities[0].countryName).toBe('Germany')
      expect(cities[0].region?.countryName).toBe('Germany')

      const districts = await districtRepo.find({ include: { city: true } })
      expect(districts.length).toBe(1)
      expect(districts[0].cityName).toBe('Munich')
      expect(districts[0].regionName).toBe('Bavaria')
      expect(districts[0].city?.name).toBe('Munich')
    })

    it('should filter by nested @JOIN field (countryName via region)', async () => {
      const continentRepo = await createEntity(knex, remult, base, Continent)
      const countryRepo = await createEntity(knex, remult, base, Country2)
      const regionRepo = await createEntity(knex, remult, base, Region2)
      const cityRepo = await createEntity(knex, remult, base, City2)

      await continentRepo.insert([
        { id: 1, name: 'Europe' },
        { id: 2, name: 'Asia' },
      ])
      await countryRepo.insert([
        { id: 1, name: 'Germany', code: 'DE', continentId: 1 },
        { id: 2, name: 'Japan', code: 'JP', continentId: 2 },
      ])
      await regionRepo.insert([
        { id: 1, name: 'Bavaria', countryId: 1 },
        { id: 2, name: 'Kanto', countryId: 2 },
      ])
      await cityRepo.insert([
        { id: 1, name: 'Munich', population: 1500000, regionId: 1 },
        { id: 2, name: 'Tokyo', population: 14000000, regionId: 2 },
      ])

      const germanCities = await cityRepo.find({
        where: { countryName: 'Germany' },
        include: { region: true },
      })
      expect(germanCities.length).toBe(1)
      expect(germanCities[0].name).toBe('Munich')
      expect(germanCities[0].countryName).toBe('Germany')
    })

    it('should handle nulls in 4-level chain', async () => {
      const continentRepo = await createEntity(knex, remult, base, Continent)
      const countryRepo = await createEntity(knex, remult, base, Country2)
      const regionRepo = await createEntity(knex, remult, base, Region2)
      const cityRepo = await createEntity(knex, remult, base, City2)
      const districtRepo = await createEntity(knex, remult, base, District)

      await continentRepo.insert({ id: 1, name: 'Europe' })
      await countryRepo.insert({ id: 1, name: 'Germany', code: 'DE', continentId: 1 })
      await regionRepo.insert({ id: 1, name: 'Bavaria', countryId: 1 })
      await cityRepo.insert({ id: 1, name: 'OrphanCity', population: 1000, regionId: 999 })
      await districtRepo.insert({ id: 1, name: 'OrphanDistrict', area: 5, cityId: 1 })

      const districts = await districtRepo.find({ include: { city: true } })
      expect(districts.length).toBe(1)
      expect(districts[0].city?.name).toBe('OrphanCity')
      expect(districts[0].city?.region).toBeFalsy()
    })
  })

  describe('Multiple Relations to Same Entity', () => {
    it('should handle multiple relations to same entity type', async () => {
      const addressRepo = await createEntity(knex, remult, base, Address)
      const personRepo = await createEntity(knex, remult, base, Person)

      await addressRepo.insert([
        { id: 1, street: '123 Home St', city: 'NYC', country: 'USA' },
        { id: 2, street: '456 Work Ave', city: 'Boston', country: 'USA' },
        { id: 3, street: '789 Bill Blvd', city: 'Chicago', country: 'USA' },
      ])
      await personRepo.insert({
        id: 1,
        name: 'John Doe',
        homeAddressId: 1,
        workAddressId: 2,
        billingAddressId: 3,
      })

      const persons = await personRepo.find({
        include: { homeAddress: true, workAddress: true, billingAddress: true },
      })
      expect(persons.length).toBe(1)
      expect(persons[0].homeCity).toBe('NYC')
      expect(persons[0].workCity).toBe('Boston')
      expect(persons[0].homeAddress?.city).toBe('NYC')
      expect(persons[0].workAddress?.city).toBe('Boston')
      expect(persons[0].billingAddress?.city).toBe('Chicago')
    })

    it('should filter by multiple @JOIN fields from same entity type', async () => {
      const addressRepo = await createEntity(knex, remult, base, Address)
      const personRepo = await createEntity(knex, remult, base, Person)

      await addressRepo.insert([
        { id: 1, street: '123 Home St', city: 'NYC', country: 'USA' },
        { id: 2, street: '456 Work Ave', city: 'NYC', country: 'USA' },
        { id: 3, street: '789 Work St', city: 'Boston', country: 'USA' },
        { id: 4, street: '101 Home Ave', city: 'LA', country: 'USA' },
      ])
      await personRepo.insert([
        { id: 1, name: 'John', homeAddressId: 1, workAddressId: 2, billingAddressId: null },
        { id: 2, name: 'Jane', homeAddressId: 4, workAddressId: 3, billingAddressId: null },
      ])

      const nycResidents = await personRepo.find({
        where: { homeCity: 'NYC', workCity: 'NYC' },
        include: { homeAddress: true, workAddress: true },
      })
      expect(nycResidents.length).toBe(1)
      expect(nycResidents[0].name).toBe('John')
    })

    it('should handle null optional relations', async () => {
      const addressRepo = await createEntity(knex, remult, base, Address)
      const personRepo = await createEntity(knex, remult, base, Person)

      await addressRepo.insert([
        { id: 1, street: '123 Home St', city: 'NYC', country: 'USA' },
        { id: 2, street: '456 Work Ave', city: 'Boston', country: 'USA' },
      ])
      await personRepo.insert({
        id: 1,
        name: 'John Doe',
        homeAddressId: 1,
        workAddressId: 2,
        billingAddressId: null,
      })

      const persons = await personRepo.find({
        include: { homeAddress: true, workAddress: true, billingAddress: true },
      })
      expect(persons[0].billingAddress).toBeNull()
      expect(persons[0].homeAddress?.city).toBe('NYC')
    })
  })

  describe('Unicode and Special Characters', () => {
    it('should handle unicode in relation fields', async () => {
      const unicodeRepo = await createEntity(knex, remult, base, UnicodeEntity)
      const refRepo = await createEntity(knex, remult, base, UnicodeRefEntity)

      await unicodeRepo.insert({
        id: 1,
        name: 'Привет мир 你好世界',
        description: 'Emoji test: 🚀🔥💯 café naïve',
      })
      await refRepo.insert({ id: 1, title: 'Reference', unicodeId: 1 })

      const refs = await refRepo.find({ include: { unicode: true } })
      expect(refs.length).toBe(1)
      expect(refs[0].unicodeName).toBe('Привет мир 你好世界')
      expect(refs[0].unicodeDescription).toBe('Emoji test: 🚀🔥💯 café naïve')
    })

    it('should filter by unicode @JOIN field', async () => {
      const unicodeRepo = await createEntity(knex, remult, base, UnicodeEntity)
      const refRepo = await createEntity(knex, remult, base, UnicodeRefEntity)

      await unicodeRepo.insert([
        { id: 1, name: 'English', description: 'Plain text' },
        { id: 2, name: '日本語', description: 'Japanese text' },
        { id: 3, name: 'العربية', description: 'Arabic text' },
      ])
      await refRepo.insert([
        { id: 1, title: 'Ref1', unicodeId: 1 },
        { id: 2, title: 'Ref2', unicodeId: 2 },
        { id: 3, title: 'Ref3', unicodeId: 3 },
      ])

      const japaneseRefs = await refRepo.find({
        where: { unicodeName: '日本語' },
        include: { unicode: true },
      })
      expect(japaneseRefs.length).toBe(1)
      expect(japaneseRefs[0].title).toBe('Ref2')
    })

    it('should handle special SQL characters in @JOIN fields', async () => {
      const unicodeRepo = await createEntity(knex, remult, base, UnicodeEntity)
      const refRepo = await createEntity(knex, remult, base, UnicodeRefEntity)

      await unicodeRepo.insert({
        id: 1,
        name: "O'Brien's \"Special\" Company",
        description: "100% discount; DROP TABLE--",
      })
      await refRepo.insert({ id: 1, title: 'Test', unicodeId: 1 })

      const refs = await refRepo.find({ include: { unicode: true } })
      expect(refs[0].unicodeName).toBe("O'Brien's \"Special\" Company")
      expect(refs[0].unicodeDescription).toBe("100% discount; DROP TABLE--")
    })
  })

  describe('Self-Referential Deep Hierarchy', () => {
    it('should handle 5-level org hierarchy', async () => {
      const orgRepo = await createEntity(knex, remult, base, OrgUnit)

      await orgRepo.insert([
        { id: 1, name: 'Corp', code: 'CORP', level: 0, parentId: null },
        { id: 2, name: 'Division A', code: 'DIV-A', level: 1, parentId: 1 },
        { id: 3, name: 'Department 1', code: 'DEP-1', level: 2, parentId: 2 },
        { id: 4, name: 'Team Alpha', code: 'TEAM-A', level: 3, parentId: 3 },
        { id: 5, name: 'Squad 1', code: 'SQ-1', level: 4, parentId: 4 },
      ])

      const units = await orgRepo.find({
        include: { parent: true },
        orderBy: { level: 'asc' },
      })

      expect(units.length).toBe(5)
      expect(units[0].parent).toBeNull()
      expect(units[1].parentName).toBe('Corp')
      expect(units[2].parentName).toBe('Division A')
      expect(units[4].parentName).toBe('Team Alpha')
      expect(units[4].parentCode).toBe('TEAM-A')
    })

    it('should filter self-referential by parent @JOIN field', async () => {
      const orgRepo = await createEntity(knex, remult, base, OrgUnit)

      await orgRepo.insert([
        { id: 1, name: 'Corp', code: 'CORP', level: 0, parentId: null },
        { id: 2, name: 'Division A', code: 'DIV-A', level: 1, parentId: 1 },
        { id: 3, name: 'Division B', code: 'DIV-B', level: 1, parentId: 1 },
        { id: 4, name: 'Department 1', code: 'DEP-1', level: 2, parentId: 2 },
        { id: 5, name: 'Department 2', code: 'DEP-2', level: 2, parentId: 3 },
      ])

      const divAChildren = await orgRepo.find({
        where: { parentName: 'Division A' },
        include: { parent: true },
      })
      expect(divAChildren.length).toBe(1)
      expect(divAChildren[0].name).toBe('Department 1')
    })
  })

  describe('Complex Filter Combinations', () => {
    it('should handle $or with multiple @JOIN fields', async () => {
      const companyRepo = await createEntity(knex, remult, base, Company)
      const employeeRepo = await createEntity(knex, remult, base, Employee)
      const saleRepo = await createEntity(knex, remult, base, Sale)

      await companyRepo.insert([
        { id: 1, name: 'Alpha Corp', city: 'NYC' },
        { id: 2, name: 'Beta Inc', city: 'LA' },
        { id: 3, name: 'Gamma LLC', city: 'Chicago' },
      ])
      await employeeRepo.insert([
        { id: 1, firstName: 'John', lastName: 'Doe', companyId: 1, managerId: null },
        { id: 2, firstName: 'Jane', lastName: 'Smith', companyId: 2, managerId: null },
      ])
      await saleRepo.insert([
        { id: 1, amount: 1000, saleDate: new Date(), status: 'completed', customerId: 1, salesRepId: 1 },
        { id: 2, amount: 2000, saleDate: new Date(), status: 'pending', customerId: 2, salesRepId: 2 },
        { id: 3, amount: 3000, saleDate: new Date(), status: 'completed', customerId: 3, salesRepId: 1 },
      ])

      const sales = await saleRepo.find({
        where: {
          $or: [{ customerCity: 'NYC' }, { salesRepFirstName: 'Jane' }],
        },
        include: { customer: true, salesRep: true },
      })
      expect(sales.length).toBe(2)
    })

    it('should handle nested $and with @JOIN fields', async () => {
      const companyRepo = await createEntity(knex, remult, base, Company)
      const employeeRepo = await createEntity(knex, remult, base, Employee)
      const saleRepo = await createEntity(knex, remult, base, Sale)

      await companyRepo.insert([
        { id: 1, name: 'Alpha Corp', city: 'NYC' },
        { id: 2, name: 'Beta Inc', city: 'NYC' },
      ])
      await employeeRepo.insert([
        { id: 1, firstName: 'John', lastName: 'Doe', companyId: 1, managerId: null },
        { id: 2, firstName: 'Jane', lastName: 'Smith', companyId: 2, managerId: null },
      ])
      await saleRepo.insert([
        { id: 1, amount: 5000, saleDate: new Date(), status: 'completed', customerId: 1, salesRepId: 1 },
        { id: 2, amount: 1000, saleDate: new Date(), status: 'completed', customerId: 2, salesRepId: 2 },
        { id: 3, amount: 8000, saleDate: new Date(), status: 'pending', customerId: 1, salesRepId: 1 },
      ])

      const sales = await saleRepo.find({
        where: {
          status: 'completed',
          customerCity: 'NYC',
          amount: { $gt: 2000 },
        },
        include: { customer: true },
      })
      expect(sales.length).toBe(1)
      expect(sales[0].amount).toBe(5000)
    })

    it('should handle $ne on @JOIN field', async () => {
      const companyRepo = await createEntity(knex, remult, base, Company)
      const orderRepo = await createEntity(knex, remult, base, Order)

      await companyRepo.insert([
        { id: 1, name: 'Alpha Corp', city: 'NYC' },
        { id: 2, name: 'Beta Inc', city: 'LA' },
        { id: 3, name: 'Gamma LLC', city: 'Chicago' },
      ])
      await orderRepo.insert([
        { id: 1, orderNumber: 'ORD-001', orderDate: new Date(), customerId: 1 },
        { id: 2, orderNumber: 'ORD-002', orderDate: new Date(), customerId: 2 },
        { id: 3, orderNumber: 'ORD-003', orderDate: new Date(), customerId: 3 },
      ])

      const nonNycOrders = await orderRepo.find({
        where: { customerCity: { $ne: 'NYC' } },
        include: { customer: true },
      })
      expect(nonNycOrders.length).toBe(2)
      expect(nonNycOrders.every((o) => o.customerCity !== 'NYC')).toBe(true)
    })

    it('should handle $contains on @JOIN field', async () => {
      const companyRepo = await createEntity(knex, remult, base, Company)
      const orderRepo = await createEntity(knex, remult, base, Order)

      await companyRepo.insert([
        { id: 1, name: 'Alpha Corporation', city: 'NYC' },
        { id: 2, name: 'Beta Industries', city: 'LA' },
        { id: 3, name: 'Gamma Corp', city: 'Chicago' },
      ])
      await orderRepo.insert([
        { id: 1, orderNumber: 'ORD-001', orderDate: new Date(), customerId: 1 },
        { id: 2, orderNumber: 'ORD-002', orderDate: new Date(), customerId: 2 },
        { id: 3, orderNumber: 'ORD-003', orderDate: new Date(), customerId: 3 },
      ])

      const corpOrders = await orderRepo.find({
        where: { customerName: { $contains: 'Corp' } },
        include: { customer: true },
      })
      expect(corpOrders.length).toBe(2)
    })

    it('should handle isIn filter on @JOIN field', async () => {
      const companyRepo = await createEntity(knex, remult, base, Company)
      const orderRepo = await createEntity(knex, remult, base, Order)

      await companyRepo.insert([
        { id: 1, name: 'Alpha', city: 'NYC' },
        { id: 2, name: 'Beta', city: 'LA' },
        { id: 3, name: 'Gamma', city: 'Chicago' },
        { id: 4, name: 'Delta', city: 'Boston' },
      ])
      await orderRepo.insert([
        { id: 1, orderNumber: 'ORD-001', orderDate: new Date(), customerId: 1 },
        { id: 2, orderNumber: 'ORD-002', orderDate: new Date(), customerId: 2 },
        { id: 3, orderNumber: 'ORD-003', orderDate: new Date(), customerId: 3 },
        { id: 4, orderNumber: 'ORD-004', orderDate: new Date(), customerId: 4 },
      ])

      const coastalOrders = await orderRepo.find({
        where: { customerCity: ['NYC', 'LA', 'Boston'] },
        include: { customer: true },
        orderBy: { id: 'asc' },
      })
      expect(coastalOrders.length).toBe(3)
      expect(coastalOrders.map((o) => o.customerCity).sort()).toEqual(['Boston', 'LA', 'NYC'])
    })
  })

  describe('findFirst Edge Cases', () => {
    it('should handle findFirst with relations when no match', async () => {
      const companyRepo = await createEntity(knex, remult, base, Company)
      const employeeRepo = await createEntity(knex, remult, base, Employee)

      await companyRepo.insert({ id: 1, name: 'Corp', city: 'NYC' })
      await employeeRepo.insert({ id: 1, firstName: 'John', lastName: 'Doe', companyId: 1, managerId: null })

      const notFound = await employeeRepo.findFirst(
        { firstName: 'NonExistent' },
        { include: { company: true } },
      )
      expect(notFound).toBeUndefined()
    })

    it('should handle findFirst with multiple relations', async () => {
      const companyRepo = await createEntity(knex, remult, base, Company)
      const employeeRepo = await createEntity(knex, remult, base, Employee)

      await companyRepo.insert({ id: 1, name: 'Corp', city: 'NYC' })
      await employeeRepo.insert([
        { id: 1, firstName: 'Boss', lastName: 'Man', companyId: 1, managerId: null },
        { id: 2, firstName: 'Worker', lastName: 'Bee', companyId: 1, managerId: 1 },
      ])

      const worker = await employeeRepo.findFirst(
        { firstName: 'Worker' },
        { include: { company: true, manager: true } },
      )
      expect(worker).toBeDefined()
      expect(worker?.manager?.firstName).toBe('Boss')
      expect(worker?.company?.name).toBe('Corp')
    })

    it('should handle findId with relations', async () => {
      const companyRepo = await createEntity(knex, remult, base, Company)
      const employeeRepo = await createEntity(knex, remult, base, Employee)

      await companyRepo.insert({ id: 1, name: 'Corp', city: 'NYC' })
      await employeeRepo.insert({ id: 1, firstName: 'John', lastName: 'Doe', companyId: 1, managerId: null })

      const employee = await employeeRepo.findId(1, { include: { company: true } })
      expect(employee).toBeDefined()
      expect(employee?.company?.name).toBe('Corp')
    })
  })

  describe('Concurrent Operations', () => {
    it('should handle parallel finds with relations', async () => {
      const companyRepo = await createEntity(knex, remult, base, Company)
      const employeeRepo = await createEntity(knex, remult, base, Employee)

      await companyRepo.insert({ id: 1, name: 'Corp', city: 'NYC' })
      await employeeRepo.insert(
        Array.from({ length: 10 }, (_, i) => ({
          id: i + 1,
          firstName: `Employee${i + 1}`,
          lastName: `Last${i + 1}`,
          companyId: 1,
          managerId: null,
        })),
      )

      const results = await Promise.all([
        employeeRepo.find({ include: { company: true } }),
        employeeRepo.find({ include: { company: true }, where: { id: { $lte: 5 } } }),
        employeeRepo.find({ include: { company: true }, where: { id: { $gt: 5 } } }),
        employeeRepo.count({ company: { $id: 1 } }),
      ])

      expect(results[0].length).toBe(10)
      expect(results[1].length).toBe(5)
      expect(results[2].length).toBe(5)
      expect(results[3]).toBe(10)
    })

    it('should handle parallel inserts and finds', async () => {
      const companyRepo = await createEntity(knex, remult, base, Company)

      await Promise.all([
        companyRepo.insert({ id: 1, name: 'Company1', city: 'NYC' }),
        companyRepo.insert({ id: 2, name: 'Company2', city: 'LA' }),
        companyRepo.insert({ id: 3, name: 'Company3', city: 'Chicago' }),
      ])

      const [all, nyc, count] = await Promise.all([
        companyRepo.find(),
        companyRepo.find({ where: { city: 'NYC' } }),
        companyRepo.count(),
      ])

      expect(all.length).toBe(3)
      expect(nyc.length).toBe(1)
      expect(count).toBe(3)
    })
  })

  describe('Sorting by Multiple @JOIN Fields', () => {
    it('should sort by multiple @JOIN computed fields', async () => {
      const companyRepo = await createEntity(knex, remult, base, Company)
      const orderRepo = await createEntity(knex, remult, base, Order)

      await companyRepo.insert([
        { id: 1, name: 'Alpha', city: 'NYC' },
        { id: 2, name: 'Beta', city: 'LA' },
        { id: 3, name: 'Alpha', city: 'Chicago' },
      ])
      await orderRepo.insert([
        { id: 1, orderNumber: 'ORD-001', orderDate: new Date(), customerId: 1 },
        { id: 2, orderNumber: 'ORD-002', orderDate: new Date(), customerId: 2 },
        { id: 3, orderNumber: 'ORD-003', orderDate: new Date(), customerId: 3 },
      ])

      const orders = await orderRepo.find({
        orderBy: { customerName: 'asc', customerCity: 'asc' },
        include: { customer: true },
      })

      expect(orders.length).toBe(3)
      expect(orders[0].customerName).toBe('Alpha')
      expect(orders[0].customerCity).toBe('Chicago')
      expect(orders[1].customerName).toBe('Alpha')
      expect(orders[1].customerCity).toBe('NYC')
      expect(orders[2].customerName).toBe('Beta')
    })
  })

  describe('Count with Complex Filters', () => {
    it('should count with $or on @JOIN fields', async () => {
      const companyRepo = await createEntity(knex, remult, base, Company)
      const orderRepo = await createEntity(knex, remult, base, Order)

      await companyRepo.insert([
        { id: 1, name: 'Alpha', city: 'NYC' },
        { id: 2, name: 'Beta', city: 'LA' },
        { id: 3, name: 'Gamma', city: 'Chicago' },
      ])
      await orderRepo.insert([
        { id: 1, orderNumber: 'ORD-001', orderDate: new Date(), customerId: 1 },
        { id: 2, orderNumber: 'ORD-002', orderDate: new Date(), customerId: 2 },
        { id: 3, orderNumber: 'ORD-003', orderDate: new Date(), customerId: 3 },
      ])

      const count = await orderRepo.count({
        $or: [{ customerCity: 'NYC' }, { customerCity: 'LA' }],
      })
      expect(count).toBe(2)
    })

    it('should count with nested relation filter', async () => {
      const countryRepo = await createEntity(knex, remult, base, Country)
      const regionRepo = await createEntity(knex, remult, base, Region)
      const cityRepo = await createEntity(knex, remult, base, City)

      await countryRepo.insert([
        { id: 1, name: 'USA', continent: 'North America' },
        { id: 2, name: 'Canada', continent: 'North America' },
      ])
      await regionRepo.insert([
        { id: 1, name: 'California', countryId: 1 },
        { id: 2, name: 'Ontario', countryId: 2 },
        { id: 3, name: 'Texas', countryId: 1 },
      ])
      await cityRepo.insert([
        { id: 1, name: 'LA', population: 4000000, regionId: 1 },
        { id: 2, name: 'Toronto', population: 2700000, regionId: 2 },
        { id: 3, name: 'SF', population: 900000, regionId: 1 },
        { id: 4, name: 'Houston', population: 2300000, regionId: 3 },
      ])

      const californiaCount = await cityRepo.count({ region: { $id: 1 } })
      expect(californiaCount).toBe(2)

      const usCount = await cityRepo.count({
        $or: [{ region: { $id: 1 } }, { region: { $id: 3 } }],
      })
      expect(usCount).toBe(3)
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
        server: '192.168.8.150',
        database: 'master',
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

describe('OptimizedDataProvider Advanced - PostgreSQL', () => {
  runAdvancedOptimizedProviderTests(() =>
    Knex.default({
      client: 'pg',
      connection: postgresConnection,
    }),
  )
})

describe('OptimizedDataProvider Advanced - MSSQL', () => {
  runAdvancedOptimizedProviderTests(() =>
    Knex.default({
      client: 'mssql',
      connection: {
        server: '192.168.8.150',
        database: 'master',
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

describe('OptimizedDataProvider Extreme - PostgreSQL', () => {
  runExtremeEdgeCaseTests(() =>
    Knex.default({
      client: 'pg',
      connection: postgresConnection,
    }),
  )
})

describe('OptimizedDataProvider Extreme - MSSQL', () => {
  runExtremeEdgeCaseTests(() =>
    Knex.default({
      client: 'mssql',
      connection: {
        server: '192.168.8.150',
        database: 'master',
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

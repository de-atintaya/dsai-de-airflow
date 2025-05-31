// MongoDB initialization script
// This script runs when the MongoDB container starts for the first time

// Switch to the airflow_db database
db = db.getSiblingDB('airflow_db');

db.accounts.insertMany([
  {
    account_id: 'A1001',
    customer_id: 'C020',
    account_type: 'savings',
    currency: 'USD',
    open_date: new Date('2019-11-27'),
    status: 'active'
  },
  {
    account_id: 'A1002',
    customer_id: 'C003',
    account_type: 'checking',
    currency: 'USD',
    open_date: new Date('2017-04-14'),
    status: 'active'
  },
  {
    account_id: 'A1003',
    customer_id: 'C012',
    account_type: 'checking',
    currency: 'USD',
    open_date: new Date('2019-01-16'),
    status: 'active'
  },
  {
    account_id: 'A1004',
    customer_id: 'C018',
    account_type: 'checking',
    currency: 'USD',
    open_date: new Date('2021-08-15'),
    status: 'closed'
  },
  {
    account_id: 'A1005',
    customer_id: 'C002',
    account_type: 'savings',
    currency: 'PEN',
    open_date: new Date('2018-03-12'),
    status: 'closed'
  },
  {
    account_id: 'A1006',
    customer_id: 'C001',
    account_type: 'checking',
    currency: 'USD',
    open_date: new Date('2020-12-13'),
    status: 'active'
  },
  {
    account_id: 'A1007',
    customer_id: 'C011',
    account_type: 'savings',
    currency: 'PEN',
    open_date: new Date('2022-07-17'),
    status: 'active'
  },
  {
    account_id: 'A1008',
    customer_id: 'C008',
    account_type: 'savings',
    currency: 'CLP',
    open_date: new Date('2016-05-08'),
    status: 'closed'
  },
  {
    account_id: 'A1009',
    customer_id: 'C003',
    account_type: 'savings',
    currency: 'USD',
    open_date: new Date('2017-01-02'),
    status: 'closed'
  },
  {
    account_id: 'A1010',
    customer_id: 'C004',
    account_type: 'checking',
    currency: 'CLP',
    open_date: new Date('2017-10-13'),
    status: 'closed'
  },
  {
    account_id: 'A1011',
    customer_id: 'C004',
    account_type: 'checking',
    currency: 'CLP',
    open_date: new Date('2019-01-06'),
    status: 'active'
  },
  {
    account_id: 'A1012',
    customer_id: 'C020',
    account_type: 'checking',
    currency: 'USD',
    open_date: new Date('2016-01-24'),
    status: 'active'
  },
  {
    account_id: 'A1013',
    customer_id: 'C020',
    account_type: 'checking',
    currency: 'USD',
    open_date: new Date('2016-07-20'),
    status: 'active'
  },
  {
    account_id: 'A1014',
    customer_id: 'C010',
    account_type: 'savings',
    currency: 'PEN',
    open_date: new Date('2015-09-30'),
    status: 'active'
  },
  {
    account_id: 'A1015',
    customer_id: 'C002',
    account_type: 'checking',
    currency: 'USD',
    open_date: new Date('2017-05-30'),
    status: 'active'
  },
  {
    account_id: 'A1016',
    customer_id: 'C003',
    account_type: 'savings',
    currency: 'PEN',
    open_date: new Date('2018-07-02'),
    status: 'active'
  },
  {
    account_id: 'A1017',
    customer_id: 'C003',
    account_type: 'checking',
    currency: 'CLP',
    open_date: new Date('2017-04-30'),
    status: 'closed'
  },
  {
    account_id: 'A1018',
    customer_id: 'C017',
    account_type: 'savings',
    currency: 'PEN',
    open_date: new Date('2021-12-12'),
    status: 'closed'
  },
  {
    account_id: 'A1019',
    customer_id: 'C019',
    account_type: 'checking',
    currency: 'USD',
    open_date: new Date('2016-11-06'),
    status: 'closed'
  },
  {
    account_id: 'A1020',
    customer_id: 'C012',
    account_type: 'savings',
    currency: 'USD',
    open_date: new Date('2021-08-03'),
    status: 'active'
  },
  {
    account_id: 'A1021',
    customer_id: 'C016',
    account_type: 'checking',
    currency: 'PEN',
    open_date: new Date('2017-04-08'),
    status: 'active'
  },
  {
    account_id: 'A1022',
    customer_id: 'C001',
    account_type: 'checking',
    currency: 'PEN',
    open_date: new Date('2015-11-30'),
    status: 'active'
  },
  {
    account_id: 'A1023',
    customer_id: 'C012',
    account_type: 'savings',
    currency: 'USD',
    open_date: new Date('2015-08-30'),
    status: 'closed'
  },
  {
    account_id: 'A1024',
    customer_id: 'C002',
    account_type: 'savings',
    currency: 'PEN',
    open_date: new Date('2018-08-09'),
    status: 'active'
  },
  {
    account_id: 'A1025',
    customer_id: 'C002',
    account_type: 'savings',
    currency: 'PEN',
    open_date: new Date('2018-06-10'),
    status: 'active'
  }
]);

db.accounts.createIndex({ account_id: 1 }, { unique: true });
db.accounts.createIndex({ customer_id: 1 });
db.accounts.createIndex({ status: 1 });
db.accounts.createIndex({ account_type: 1 });
db.accounts.createIndex({ currency: 1 });

print('Successfully created accounts collection with ' + db.accounts.countDocuments() + ' documents');
print('Indexes created for account_id, customer_id, status, account_type, and currency');
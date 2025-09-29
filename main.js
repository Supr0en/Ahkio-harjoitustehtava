
import fs from "fs"; // fs import for reading files.
import fsPromises from "fs/promises"; // fsPromises import for reading .sql.
import sqlite3 from "sqlite3"; // sqlite3 import to create database.
import csv from "csv-parser"; // csv import for reading .csv file.
import createCsvWriter from "csv-writer"; // createCsvWriter import for generating .csv file and adding data inside.

const db = new sqlite3.Database('your-database.db'); // Initializing database.

// Reading json to get tax rules.
const data = fs.readFileSync("tax_rules.json", "utf8")
const jsonData = JSON.parse(data).vat;

// Table names listed to be used in checking if tables exist and to insert table content.
const tableNames = [
   'customers', 'products', 'stock_levels', 'orders', 'order_lines'
];

// Function creates tables into db and populates with data.
function createTableAndPopulate() {
   return new Promise(async (resolve, reject) => {
      db.serialize(async () => {
         db.run("PRAGMA foreign_keys = OFF;"); // added to let data to be added when one order_id has multiple products in order_lines.
         db.run("BEGIN TRANSACTION;");
         tableNames.forEach(table => {db.run(`DROP TABLE IF EXISTS ${table}`)}); // checking if tables exist and dropping to avoid errors.
         const schema = await fsPromises.readFile("db-schema.sql", "utf8")
         db.exec(schema); // generated tables into db.
         // forEach loops through table names and populates relative table.
         tableNames.forEach(file => {
            return new Promise((resolveFile, rejectFile) => {
               let columns = [];
               fs.createReadStream(`${file}.csv`).pipe(csv()).on("headers", headers => {
                  columns = headers;
               })
               .on("data", row => {
                  const values = columns.map(col => {
                     let val = row[col];
                     if (typeof val === "string") val = val.trim();
                     return val;
                  });
                  const placeholders = columns.map(() => "?").join(", ");
                  const headersList = columns.map(col => `"${col.replace(/"/g, '""')}"`).join(", ");
                  db.run(`INSERT INTO ${file} (${headersList}) VALUES (${placeholders})`, values);
               })
               .on("end", () => {
                  console.log(`Data added to table ${file}`);
                  resolveFile();
               })
               .on("error", rejectFile);
            });
         });
         db.run("COMMIT;", (err) => {
            if (err) return reject(err);
            resolve();
         })
      });
   })
}

// Creates csv file and adds headers.
const csvWriter = createCsvWriter.createObjectCsvWriter({
   path: 'order_totals.csv',
   header: [
      {id: 'order_id', title: "order_id"},
      {id: 'customer_name', title: "customer_name"},
      {id: 'net_total', title: "net_total"},
      {id: 'vat_total', title: 'vat_total'},
      {id: 'gross_total', title: "gross_total"},
      {id: 'is_fully_in_stock', title: "is_fully_in_stock"},
   ]
});

// Function gets orderData for each order_id from orders table.
function getOrderInfo() {
   return new Promise((resolve, reject) => {
      const orders = [];
      db.all(`SELECT * FROM orders JOIN customers ON orders.customer_id = customers.customer_id`, [], (err, order) => {
         if (err) throw reject(err);
         Promise.all(order.map((items) => {
            return new Promise((resolveItem) => {
               let orderObject = {
                  order_id: items.order_id,
                  customer_name: items.customer_name,
                  net_total: 0.0,
                  vat_total: 0.0,
                  gross_total: 0.0,
                  is_fully_in_stock: true,
               }
               db.all(`SELECT * FROM order_lines JOIN products ON order_lines.sku = products.sku JOIN stock_levels ON order_lines.sku = stock_levels.sku WHERE order_lines.order_id = ? ORDER BY order_lines.sku`, [orderObject.order_id], (err, itemOrder) => {
                  if(err) throw err;
                  let brutto_total = 0;
                  let net_total = 0;
                  itemOrder.forEach(item => {
                     if (item.qty >= item.qty_on_hand) orderObject = {...orderObject, is_fully_in_stock: false};
                     let bruttoValue = item.unit_price * item.qty;
                     let netValue = 0;
                     switch (item.vat_code) {
                        case "STANDARD": netValue = (bruttoValue / (1 + jsonData.STANDARD)); break;
                        case "REDUCED": netValue = (bruttoValue / (1 + jsonData.REDUCED)); break;
                        case "ZERO": netValue = (bruttoValue / (1 + jsonData.ZERO)); break;
                     }
                     net_total += netValue;
                     brutto_total += bruttoValue;
                  });
                  orderObject.gross_total = brutto_total.toFixed(2);
                  orderObject.net_total = net_total.toFixed(2);
                  orderObject.vat_total = (brutto_total - net_total).toFixed(2);

                  orders.push(orderObject);
                  resolveItem();
               })
            });
         })).then(() => resolve(orders)).catch(reject);
      });
   })
}

// Async function to ensure everything are done in correct order.
(async () => {
   await createTableAndPopulate();
   const orders = await getOrderInfo();
   await csvWriter.writeRecords(orders).then(() => {
      console.log('CSV added to folder!');
   })
})();

// Run by using "node main.js" inside terminal.

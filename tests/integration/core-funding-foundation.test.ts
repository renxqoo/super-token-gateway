import { expect, test } from "bun:test";
import { migrate } from "@gateway/storage";
import { withDatabase } from "../support/database.ts";

test("a frozen account cannot accumulate more than NUMERIC(38,0) of reservation across otherwise valid cells", () =>
  withDatabase(async (db) => {
    await migrate(db);
    const brand = crypto.randomUUID();
    const account = crypto.randomUUID();
    await db`INSERT INTO supplier_brands(id,name) VALUES(${brand},${brand})`;
    await db`INSERT INTO upstream_accounts(id,brand_id,name,state) VALUES(${account},${brand},${account},'DISABLED')`;
    await db`INSERT INTO upstream_account_balances(account_id,currency) VALUES(${account},'USD')`;
    await db.begin(async (tx) => {
      await tx`SELECT core_funding.exclusive_v1(ARRAY[${account}::uuid])`;
      await tx`INSERT INTO core_funding.balance_cells(account_id,currency,cell,reserved_atoms,spent_limit,revision_limit) VALUES(${account},'USD',0,'99999999999999999999999999999999999999',0,0)`;
      await expect(
        tx.savepoint(async (savepoint) => {
          await savepoint`INSERT INTO core_funding.balance_cells(account_id,currency,cell,reserved_atoms,spent_limit,revision_limit) VALUES(${account},'USD',1,1,0,0)`;
        }),
      ).rejects.toMatchObject({ errno: "22003" });
      expect(
        await tx`SELECT sum(reserved_atoms)::text reserved FROM core_funding.balance_cells WHERE account_id=${account}`,
      ).toEqual([{ reserved: "99999999999999999999999999999999999999" }]);
    });
  }));

test("budget grants and reclaims conserve money and revisions, replay exactly, and cannot reclaim reserved funds or leave partial writes", () =>
  withDatabase(async (db) => {
    await migrate(db);
    const brand = crypto.randomUUID();
    const account = crypto.randomUUID();
    await db`INSERT INTO supplier_brands(id,name) VALUES(${brand},${brand})`;
    await db`INSERT INTO upstream_accounts(id,brand_id,name) VALUES(${account},${brand},${account})`;
    await expect(
      db`INSERT INTO upstream_account_balances(account_id,currency,balance_atoms,reserved_atoms,spent_atoms,revision) VALUES(${account},'USD',100,80,7,11)`,
    ).rejects.toMatchObject({ errno: "P0403" });
    await db.begin(async (tx) => {
      await tx`SELECT core_funding.exclusive_v1(ARRAY[${account}::uuid])`;
      await tx`INSERT INTO upstream_account_balances(account_id,currency,balance_atoms,reserved_atoms,spent_atoms,revision) VALUES(${account},'USD',100,0,7,11)`;
    });
    await expect(
      db`SELECT core_funding.transfer_v1(${account}::uuid,'USD',0,60,100,10,'grant-0')`,
    ).rejects.toMatchObject({ errno: "P0F01" });
    await db.begin(async (tx) => {
      await tx`SELECT core_funding.exclusive_v1(ARRAY[${account}::uuid])`;
      const first =
        await tx`SELECT core_funding.transfer_v1(${account}::uuid,'USD',0,60,100,10,'grant-0') id`;
      expect(
        await tx`SELECT core_funding.transfer_v1(${account}::uuid,'USD',0,60,100,10,'grant-0') id`,
      ).toEqual(first);
      await expect(
        tx.savepoint(
          (tx) => tx`SELECT core_funding.transfer_v1(${account}::uuid,'USD',0,61,100,10,'grant-0')`,
        ),
      ).rejects.toMatchObject({ errno: "P0409" });
      await tx`SELECT core_funding.transfer_v1(${account}::uuid,'USD',1,40,100,10,'grant-1')`;
    });
    expect((await db`SELECT core_funding.audit_v1(${account}::uuid,'USD') value`)[0].value).toEqual(
      {
        balance_atoms: "100",
        reserved_atoms: "0",
        spent_atoms: "7",
        revision: "11",
        available_atoms: "100",
        adjustment_atoms: "0",
        root_reserved_atoms: "0",
        cells_conserved: true,
      },
    );
    await db.begin(async (tx) => {
      await tx`SELECT core_funding.share_v1(ARRAY[${account}::uuid])`;
      await tx`UPDATE core_funding.balance_cells SET balance_atoms=50,reserved_atoms=45,spent_atoms=10,revision=1 WHERE account_id=${account} AND currency='USD' AND cell=0`;
      await expect(
        tx.savepoint(
          (tx) =>
            tx`SELECT core_funding.transfer_v1(${account}::uuid,'USD',0,-1,0,0,'shared-reclaim')`,
        ),
      ).rejects.toMatchObject({ errno: "P0F01" });
    });
    await db.begin(async (tx) => {
      await tx`SELECT core_funding.exclusive_v1(ARRAY[${account}::uuid])`;
      await expect(
        tx.savepoint(
          (tx) =>
            tx`SELECT core_funding.transfer_v1(${account}::uuid,'USD',0,-6,0,0,'reserved-reclaim')`,
        ),
      ).rejects.toMatchObject({ errno: "P0422" });
      await tx`SELECT core_funding.transfer_v1(${account}::uuid,'USD',1,-40,0,0,'reclaim-1')`;
      await tx`SELECT core_funding.transfer_v1(${account}::uuid,'USD',0,10,0,0,'grant-0-again')`;
      await tx`SELECT core_funding.transfer_v1(${account}::uuid,'USD',0,0,-90,-9,'return-headroom')`;
      await expect(
        tx.savepoint(
          (tx) =>
            tx`SELECT core_funding.transfer_v1(${account}::uuid,'USD',0,0,-1,0,'spent-headroom')`,
        ),
      ).rejects.toMatchObject({ errno: "P0422" });
      await expect(
        tx.savepoint(
          (tx) =>
            tx`SELECT core_funding.transfer_v1(${account}::uuid,'USD',0,1,99999999999999999999999999999999999979,0,'overflow-after-root-debit')`,
        ),
      ).rejects.toMatchObject({ errno: "22003" });
    });
    expect((await db`SELECT core_funding.audit_v1(${account}::uuid,'USD') value`)[0].value).toEqual(
      {
        balance_atoms: "90",
        reserved_atoms: "45",
        spent_atoms: "17",
        revision: "12",
        available_atoms: "45",
        adjustment_atoms: "0",
        root_reserved_atoms: "0",
        cells_conserved: true,
      },
    );
    expect(
      await db`SELECT balance_atoms::text,spent_atoms::text,revision::text FROM upstream_account_balances WHERE account_id=${account} AND currency='USD'`,
    ).toEqual([{ balance_atoms: "30", spent_atoms: "7", revision: "11" }]);
    expect(
      await db`SELECT reference FROM core_funding.budget_transfers WHERE account_id=${account} AND kind<>'MIGRATION' ORDER BY reference`,
    ).toEqual([
      { reference: "grant-0" },
      { reference: "grant-0-again" },
      { reference: "grant-1" },
      { reference: "reclaim-1" },
      { reference: "return-headroom" },
    ]);
  }));

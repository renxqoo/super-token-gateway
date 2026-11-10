# 局部状态模型（可复现）

此代码仅用于挑战设计规则，使用 Python 标准库整数/Fraction；不是 Bun + TypeScript 服务、数据库适配器或生产资金实现；切换技术栈不改变本模型的语言无关规则，Bun 的精度/流/驱动需另做运行验收。所有数量是任意模型单位，不代表实际币种报价。

覆盖两个有限模型：一个订单的最终证据、截止、捕获、重复观察与平台成本；一个 grant 的发行、重复接收、源组退还、发行端确认与重复消息。检查每次有效状态变更后的守恒。乱序且前置条件未满足的消息被拒绝/不改变状态，不假定网络会自动补齐它们。

未覆盖：真实并发/锁、数据库 effect/外键/触发器、跨物理组持久性、多个订单/供应商共享资源、权限/来源真实性、真实发送权、所有故障组合或完整金融账本。模型中 invocation 数固定为一，不能把该断言当成真实网络只发送一次的证明。

运行方式：提取下列唯一 Python 代码块到临时文件执行。无网络、无业务数据写入。结果见 [验证记录](validation.md)。

```python
from fractions import Fraction
from itertools import permutations
import json

# Bounded executable model of document rules, not a service or database adapter.
class Order:
    def __init__(self, reserve=40, calculated=30):
        self.reserve = reserve
        self.calculated = calculated
        self.posted = 100
        self.reserved = reserve
        self.charge = 'RESERVED'
        self.expired = False
        self.capture_count = 0
        self.captured = 0
        self.uncollected = 0
        self.invocations = 1
        self.vendor_granted = 100
        self.vendor_free = 80
        self.vendor_held = 20
        self.vendor_provision = 0
        self.vendor_accrued = 0
        self.vendor_overrun = 0
        self.vendor_state = 'HELD'
        self.journal = [(100, 0, -100)]

    def step(self, event):
        if event == 'seal':
            if self.charge == 'RESERVED':
                if self.expired:
                    self.charge = 'NO_CHARGE'
                    self.reserved = 0
                else:
                    self.charge = 'READY'
        elif event == 'expire':
            self.expired = True
            if self.charge == 'RESERVED':
                self.charge = 'NO_CHARGE'
                self.reserved = 0
        elif event in ('capture', 'capture_duplicate'):
            if self.charge == 'READY':
                amount = min(self.reserve, self.calculated)
                self.posted -= amount
                self.reserved = 0
                self.captured = amount
                self.uncollected = self.calculated - amount
                self.capture_count += 1
                self.charge = 'CAPTURED'
                if amount:
                    self.journal.append((-amount, amount, 0))
        elif event == 'provider_cost':
            self.provider_cost(15)
        elif event == 'provision':
            if self.vendor_state == 'HELD':
                self.vendor_held = 0
                self.vendor_provision = 20
                self.vendor_state = 'PROVISIONED'
        elif event == 'retry_observe':
            # Existing intention returns a status, never a new send grant.
            pass
        else:
            raise AssertionError(event)
        self.check()

    def provider_cost(self, amount):
        # A unique evidence/adjustment is assumed here; duplicate same amount is a no-op.
        previous = self.vendor_accrued
        released = self.vendor_held + self.vendor_provision
        self.vendor_held = self.vendor_provision = 0
        self.vendor_free += released
        delta = amount - previous
        if delta >= 0:
            funded = min(delta, self.vendor_free)
            self.vendor_free -= funded
            self.vendor_overrun += delta - funded
        else:
            reduction = -delta
            repaid = min(reduction, self.vendor_overrun)
            self.vendor_overrun -= repaid
            self.vendor_free += reduction - repaid
        self.vendor_accrued = amount
        self.vendor_state = 'ACCRUED'

    def add_provider_grant(self, amount):
        self.vendor_granted += amount
        repaid = min(amount, self.vendor_overrun)
        self.vendor_overrun -= repaid
        self.vendor_free += amount - repaid

    def check(self):
        assert self.posted >= self.reserved >= 0
        expected = self.reserve if self.charge in ('RESERVED', 'READY') else 0
        assert self.reserved == expected
        assert self.posted == 100 - self.captured
        assert self.capture_count <= 1
        assert self.invocations == 1
        assert all(sum(lines) == 0 for lines in self.journal)
        assert sum(lines[0] for lines in self.journal) == self.posted
        assert self.vendor_free >= 0
        assert self.vendor_held >= 0
        assert self.vendor_provision >= 0
        assert self.vendor_granted + self.vendor_overrun == (
            self.vendor_free + self.vendor_held + self.vendor_provision + self.vendor_accrued
        )
        if self.charge == 'NO_CHARGE':
            assert self.capture_count == self.captured == self.uncollected == 0
            assert self.posted == 100
            if self.vendor_state != 'ACCRUED':
                assert self.vendor_free == 80
                assert self.vendor_held + self.vendor_provision == 20


class Grant:
    def __init__(self):
        self.free = 100
        self.issued = 0
        self.issued_once = False
        self.received = False
        self.local_returned = False
        self.root_returned = False
        self.local_granted = 0

    def step(self, event):
        if event in ('issue', 'issue_duplicate') and not self.issued_once:
            self.free -= 40
            self.issued += 40
            self.issued_once = True
        elif event in ('deliver', 'deliver_duplicate'):
            if self.issued_once and not self.received:
                self.received = True
                self.local_granted += 40
        elif event == 'return_local':
            if self.received and not self.local_returned:
                self.local_granted -= 10
                self.local_returned = True
        elif event in ('return_root', 'return_root_duplicate'):
            if self.local_returned and not self.root_returned:
                self.issued -= 10
                self.free += 10
                self.root_returned = True
        self.check()

    def check(self):
        assert self.free >= 0 and self.issued >= 0 and self.local_granted >= 0
        assert self.free + self.issued == 100
        grant_in_transit = 40 if self.issued_once and not self.received else 0
        return_in_transit = 10 if self.local_returned and not self.root_returned else 0
        assert self.issued == self.local_granted + grant_in_transit + return_in_transit


def run():
    order_events = ['seal', 'expire', 'capture', 'capture_duplicate', 'provider_cost', 'retry_observe', 'provision']
    order_paths = 0
    for path in permutations(order_events):
        order = Order()
        for event in path:
            order.step(event)
        order_paths += 1

    grant_events = ['issue', 'issue_duplicate', 'deliver', 'deliver_duplicate', 'return_local', 'return_root', 'return_root_duplicate']
    grant_paths = 0
    for path in permutations(grant_events):
        grant = Grant()
        for event in path:
            grant.step(event)
        grant_paths += 1

    # Late evidence and duplicate capture cannot revive an irrevocable customer waiver.
    order = Order()
    for event in ['expire', 'seal', 'capture', 'capture_duplicate', 'provider_cost']:
        order.step(event)
    assert order.charge == 'NO_CHARGE' and order.posted == 100 and order.vendor_accrued == 15

    # A normal capture is independent from provider accounting, including overrun.
    order = Order(calculated=60)
    order.step('seal')
    order.step('capture')
    order.provider_cost(125)
    order.check()
    assert order.captured == 40 and order.uncollected == 20 and order.vendor_overrun == 25
    order.add_provider_grant(10)
    order.check()
    assert order.vendor_free == 0 and order.vendor_overrun == 15
    order.provider_cost(90)
    order.check()
    assert order.vendor_overrun == 0 and order.vendor_free == 20

    order = Order(reserve=0, calculated=0)
    order.step('seal')
    order.step('capture')
    assert order.capture_count == 1 and order.posted == 100 and len(order.journal) == 1

    # Root-to-cell movement and reclaim never duplicate principal or reclaim a hold.
    root, cell, held = 100, 0, 0
    root, cell = root - 40, cell + 40
    held = 30
    reclaim = min(cell - held, 40)
    root, cell = root + reclaim, cell - reclaim
    assert (root, cell, held) == (70, 30, 30)
    assert root + cell == 100 and cell >= held
    cell, held = cell - 20, 0
    assert root + cell + 20 == 100 and cell >= held

    # Exact rational rates: round once on the order, never once per component.
    ceil = lambda value: -(-value.numerator // value.denominator)
    assert ceil(sum([Fraction(1, 3)] * 3)) == 1
    assert sum(ceil(Fraction(1, 3)) for _ in range(3)) == 3
    money_cases = 0
    for numerator in range(41):
        for denominator in range(1, 41):
            value = Fraction(numerator, denominator)
            result = ceil(value)
            assert result >= value and result < value + 1
            money_cases += 1

    return {'order_paths': order_paths, 'grant_paths': grant_paths, 'rational_cases': money_cases,
            'direct_scenarios': 4, 'all_passed': True}

if __name__ == '__main__':
    print(json.dumps(run()))
```

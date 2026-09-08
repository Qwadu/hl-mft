# hl-mft — план

## 0. Что показал анализ кошелька Wintermute (0xecb6...2b00)

Снято через публичный info-API за ~95 минут (15 441 филл):

| Метрика | Значение |
|---|---|
| Account value | $45.3M, 84 открытых позиций, 1 774 открытых ордера |
| Стиль | Чистый market-maker: лесенка лимиток с обеих сторон, 1–14 уровней, мелкий размер у mid (~$1–2k), крупный глубже (~$100k) |
| Maker-доля филлов | 84% (ZEC: 68%) |
| Фактические комиссии | maker **−0.30 bps (рибейт)**, taker +0.66 bps |
| Инвентарь | Нетто-шорт почти по всем перпам → хеджируются на других площадках (спот/Binance) |
| ZEC | Позиция −280 → −470 ZEC за час; квотируют ±1–2 тика от mid, размеры 0.05–2.3 ZEC ($60–$2.7k) |

**Вывод: их стратегию нельзя скопировать буквально.** Она живёт на трёх вещах, которых у нас нет:
1. Рибейт −0.3 bps за maker (у нас +1.5 bps до $5M/14д объёма; спред ZEC ≈ 1 bp — т.е. чистый spread-capture для нас убыточен по определению).
2. Кросс-биржевой хедж инвентаря.
3. Address-based rate-limit HL: **1 action на 1 USDC накопленного объёма** (стартовый буфер 10 000). С капиталом <$1k тысячи cancel/replace в час исчерпают лимит за день → 1 запрос / 10 сек.

**Что можно взять («торговать вместе с ММ»):** их лесенка — это карта, где стоит настоящая ликвидность. Edge для нас — не спред, а **краткосрочное направленное предсказание** по микроструктуре: если ММ (пассивный) вынужден набирать инвентарь против потока, цена с высокой вероятностью продолжит движение на горизонте секунды–минуты. Мы входим *в направлении* потока, пассивно (maker) где возможно, taker при сильном сигнале, и выходим за секунды–минуты.

## 1. Стратегия v1: «Flow-following micro-momentum»

Сигнал = взвешенная сумма нормализованных фич (z-score по rolling-окну), считается на каждом апдейте L2/trades:

| Фича | Что ловит |
|---|---|
| **OBI_k** = (ΣbidSz − ΣaskSz)/(ΣbidSz + ΣaskSz), k∈{1,3,5,10,20} уровней | давление в стакане |
| **Microprice − mid** (по best bid/ask, взвешенно размером) | куда «тянет» цену |
| **Trade-flow / CVD** за 1s/5s/30s (агрессивные buy − sell по `trades`) | активный поток |
| **Lead-lag**: (ref_mid − hl_mid)/hl_mid, ref = Binance-futures bookTicker (primary) / LSE tick (fallback) | HL догоняет крупные площадки |
| **Book-shape delta**: изменение суммарного размера на 5 уровнях за Δt (ММ снимает/добавляет ликвидность) | поведение ММ |
| Funding / OI (медленные, из `activeAssetCtx`) | фильтр режима, не вход |

Вход: score > +θ → long, < −θ → short. Сила сигнала определяет исполнение:
- |score| ∈ [θ, θ_taker): post-only лимитка на best bid/ask (join queue), TTL 2–5 с, если не заполнилась — отмена (не перевыставляем в цикле — экономим rate-limit).
- |score| ≥ θ_taker: IOC taker.

Выход: (а) score развернулся ниже θ_exit, (б) take-profit в тиках/bps, (в) stop по ATR-подобной волатильности (realized vol за 1 мин), (г) max hold time (60–300 с). Выход всегда IOC при (в)/(г), post-only при (а)/(б).

Параметры θ, окна, TP/SL, hold — калибруются оффлайн на записанных нами данных (см. §4), не «с потолка».

**Вселенная:** динамически, топ-N по 24h объёму и глубине (сумма размера в 10 bps от mid) из `metaAndAssetCtxs`, пересчёт раз в сутки; blacklist HIP-3 (`xyz:*`) и монет с тиком > 5 bps. ZEC — в первой пачке.

## 2. Риск (согласовано)

| Параметр | Значение |
|---|---|
| Плечо | 5x isolated на монету (задаётся ботом при старте) |
| Риск на сделку | ≤2% депо (стоп-дистанция × размер) |
| Одновременно позиций | ≤10; gross notional ≤ 8× депо |
| Kill-switch | дневной PnL ≤ −10% депо или общий drawdown ≤ −25% от high-water-mark → отмена всех ордеров, закрытие позиций IOC, стоп процесса; ручной сброс через дашборд |
| Микро-лайв | первые дни: cap notional $10–50/позицию, 1–3 монеты |
| Прочее | `reduceOnly` на выходах; проверка `min notional $10`; reconcile позиций с биржей каждые 5 с; dead-man: если WS молчит > 10 с — отмена всех ордеров; лимиты на число action-запросов/мин (защита от address rate-limit) |

## 3. Архитектура (Python 3.12, asyncio)

```
hl_mft/
  feeds/        hyperliquid_ws.py (l2Book+trades+bbo+activeAssetCtx+userFills+orderUpdates)
                binance_ws.py (bookTicker ref), lse_ws.py (fallback ref)
  book/         local L2 book, microprice, OBI
  features/     rolling z-scores, CVD, lead-lag, book-shape
  strategy/     flow_momentum.py (score → intents), universe.py
  risk/         limits, kill-switch, sizing (isolated 5x, ≤2%)
  execution/    order manager (post-only/IOC, TTL, cloid, reconcile), rate budget
  state/        SQLite (orders, fills, positions, pnl, events)
  recorder/     Parquet: L2 snapshots (каждый апдейт), trades, ref-ticks, features, decisions
  metrics/      prometheus-client → Grafana Cloud (remote_write через Grafana Agent/Alloy)
  dashboard/    FastAPI + HTMX: позиции, PnL, ордера, фичи по монете, kill-switch, правка параметров на лету
  research/     replay-бэктестер на записанных Parquet (тот же код фич/стратегии), калибровка θ
  cli.py        run / record-only / replay / calibrate
```

Один процесс, один WS к HL (лимит 10 соединений, 1000 подписок; 4 подписки × 10 монет = 40). Все параметры — pydantic-конфиг `config.yaml`, hot-reload через дашборд.

## 4. Данные для калибровки

- LSE (`lse_live_*`): композитные тики price/bid/ask по ~58 crypto-парам, replay до 24 ч, свечи глубже. Используется как **референсная цена (lead-lag)** и для проверки. Стакана HL там нет.
- **Собственный recorder** — с первого дня пишем L2 HL + trades + ref-ticks в Parquet. Через 24–48 ч записи есть данные для честной калибровки. Дополнительно: официальный S3-архив HL `s3://hyperliquid-archive/market_data/<date>/<hour>/l2Book/<coin>.lz4` (requester-pays) — исторический L2 для более длинной истории.

## 5. Этапы (каждый — отдельный PR)

1. **Скелет + feeds + recorder + метрики** — бот только слушает, пишет Parquet, экспортирует метрики. Запуск на Vultr Tokyo в Docker. (сразу начинает копить данные)
2. **Book/features/стратегия + paper-режим** — считает сигналы и «виртуальные» сделки, пишет в SQLite, показывает в дашборде. Без ордеров.
3. **Replay-бэктестер + калибровка** на записанных данных (fees 1.5/4.5 bps, задержка ~50–100 мс, очередь для post-only консервативно = fill только если цена прошла уровень).
4. **Execution + risk + kill-switch** → микро-лайв $10–50 на 1–3 монетах.
5. **Дашборд управления** (правка параметров, пауза/стоп, ручное закрытие), алерты Grafana.
6. Масштабирование: расширение вселенной, размер, подбор θ по монетам.

## 6. Инфраструктура

- Vultr Tokyo: Docker Compose (`bot`, `grafana-alloy` для push в Grafana Cloud), systemd-restart, логи JSON → Loki (Grafana Cloud free).
- Секреты: `.env` на сервере (HL_AGENT_PRIVATE_KEY, HL_ACCOUNT_ADDRESS, LSE_API_KEY, GRAFANA_CLOUD_*). В репо — только `.env.example`.
- API-agent wallet HL: создаётся в app.hyperliquid.xyz → More → API; агент подписывает ордера, не может выводить средства.

## 7. Честные ожидания

- Base-tier комиссии: taker 4.5 bps, maker 1.5 bps. Средний выигрыш на сделку должен быть > 3–6 bps после комиссий — при hold секунды–минуты это требует реального предсказательного сигнала. Если калибровка на наших данных не покажет положительного ожидания после комиссий — в лайв полным размером не идём; расширяем фичи/горизонт.
- С <$1k «обойти 99% ритейла» реально по дисциплине и инфраструктуре, но абсолютный PnL будет маленьким; цель этапов 1–4 — доказать edge на микро-размере и данных, потом масштаб.

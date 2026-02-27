<p align="center">
  <svg width="60" height="60" viewBox="0 0 100 100">
    <defs>
      <linearGradient id="snowGrad" x1="0%" y1="0%" x2="100%" y2="100%">
        <stop offset="0%" style="stop-color:#29b5e8"/>
        <stop offset="100%" style="stop-color:#0d9488"/>
      </linearGradient>
    </defs>
    <g fill="url(#snowGrad)">
      <rect x="47" y="10" width="6" height="80" rx="3"/>
      <rect x="47" y="10" width="6" height="80" rx="3" transform="rotate(60 50 50)"/>
      <rect x="47" y="10" width="6" height="80" rx="3" transform="rotate(120 50 50)"/>
      <circle cx="50" cy="50" r="8"/>
      <circle cx="50" cy="18" r="5"/><circle cx="50" cy="82" r="5"/>
      <circle cx="77" cy="34" r="5"/><circle cx="23" cy="66" r="5"/>
      <circle cx="77" cy="66" r="5"/><circle cx="23" cy="34" r="5"/>
    </g>
  </svg>
  &nbsp;&nbsp;
  <svg width="60" height="60" viewBox="0 0 100 100">
    <defs>
      <linearGradient id="tradeGrad" x1="0%" y1="100%" x2="100%" y2="0%">
        <stop offset="0%" style="stop-color:#ef4444"/>
        <stop offset="50%" style="stop-color:#f59e0b"/>
        <stop offset="100%" style="stop-color:#10b981"/>
      </linearGradient>
    </defs>
    <polyline points="10,75 30,55 50,65 70,35 90,20" fill="none" stroke="url(#tradeGrad)" stroke-width="6" stroke-linecap="round" stroke-linejoin="round"/>
    <polygon points="90,20 90,38 72,20" fill="#10b981"/>
    <circle cx="30" cy="55" r="4" fill="#f59e0b"/>
    <circle cx="50" cy="65" r="4" fill="#f59e0b"/>
    <circle cx="70" cy="35" r="4" fill="#10b981"/>
    <rect x="15" y="80" width="8" height="12" rx="2" fill="#64748b" opacity="0.5"/>
    <rect x="30" y="75" width="8" height="17" rx="2" fill="#64748b" opacity="0.5"/>
    <rect x="45" y="78" width="8" height="14" rx="2" fill="#64748b" opacity="0.5"/>
    <rect x="60" y="70" width="8" height="22" rx="2" fill="#64748b" opacity="0.5"/>
    <rect x="75" y="65" width="8" height="27" rx="2" fill="#64748b" opacity="0.5"/>
  </svg>
</p>

<h1 align="center">Snow-Trade</h1>

<p align="center"><strong>Security Trading, Settlement & Master</strong></p>

A Snowflake-native application for enterprise data management of securities, trade execution, and settlement workflows.

## Features

- **Security Master (Golden Record)** - Centralized reference data management with ISIN lookup via OpenFIGI API
- **Trade Management** - Equity and bond trade capture with hybrid tables
- **Live Market Data** - Real-time stock prices via Yahoo Finance integration
- **FIX/FIXML Support** - Stage for FIX protocol message processing
- **Streamlit Dashboard** - Interactive UI for security and trade management

## Installation

```sql
-- Run the installation script
@install_snowtrade.sql
```

## Uninstallation

```sql
-- Run the uninstallation script (WARNING: Deletes all data!)
@uninstall_snowtrade.sql
```

## Database Structure

- `SECURITY_MASTER_DB.GOLDEN_RECORD` - Security master reference and history
- `SECURITY_MASTER_DB.TRADES` - Equity and bond trade tables
- `SECURITY_MASTER_DB.SECURITIES` - SP500 and NYSE listed securities
- `SECURITY_MASTER_DB.FIXED_INCOME` - Corporate bonds

## Requirements

- Snowflake account with ACCOUNTADMIN role
- Warehouse: ADHOC_WH (or modify scripts)

## Author

Colm Moynihan - February 2026

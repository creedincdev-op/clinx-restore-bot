const BASE_PRICES_INR = {
  pro: 99,
  pro_plus: 199,
  pro_ultra: 349,
};

const FALLBACK_RATES = {
  INR: 1,
  USD: 0.0119,
  EUR: 0.0109,
  GBP: 0.0092,
  AUD: 0.0184,
  CAD: 0.0161,
  SGD: 0.0158,
  AED: 0.0438,
};

async function getCurrencyForCountry(countryCode) {
  if (!countryCode || countryCode === 'IN') {
    return 'INR';
  }

  const response = await fetch(`https://restcountries.com/v3.1/alpha/${countryCode}?fields=currencies`);
  if (!response.ok) {
    throw new Error('country lookup failed');
  }

  const data = await response.json();
  const record = Array.isArray(data) ? data[0] : data;
  const currencyCode = Object.keys(record.currencies || {})[0];
  if (!currencyCode) {
    throw new Error('currency missing');
  }

  return currencyCode.toUpperCase();
}

async function getExchangeRate(currency) {
  if (currency === 'INR') {
    return 1;
  }

  const response = await fetch(`https://api.frankfurter.app/latest?from=INR&to=${currency}`);
  if (!response.ok) {
    throw new Error('rate lookup failed');
  }

  const data = await response.json();
  const rate = data && data.rates ? data.rates[currency] : null;
  if (!rate) {
    throw new Error('rate missing');
  }

  return rate;
}

function toLocale(countryCode, currency) {
  if (countryCode === 'IN' || currency === 'INR') {
    return 'en-IN';
  }

  if (countryCode && countryCode.length === 2) {
    return `en-${countryCode}`;
  }

  return 'en-US';
}

function buildPrices(rate) {
  return Object.fromEntries(
    Object.entries(BASE_PRICES_INR).map(([key, value]) => [key, Number((value * rate).toFixed(2))])
  );
}

module.exports = async (req, res) => {
  const country = String(req.headers['x-vercel-ip-country'] || 'US').toUpperCase();
  let currency = 'INR';

  res.setHeader('Content-Type', 'application/json; charset=utf-8');
  res.setHeader('Cache-Control', 's-maxage=3600, stale-while-revalidate=86400');

  try {
    currency = await getCurrencyForCountry(country);
    let rate;

    try {
      rate = await getExchangeRate(currency);
    } catch (error) {
      rate = FALLBACK_RATES[currency];
    }

    if (!rate) {
      currency = country === 'IN' ? 'INR' : 'USD';
      rate = FALLBACK_RATES[currency] || 1;
    }

    res.status(200).json({
      country,
      currency,
      locale: toLocale(country, currency),
      prices: buildPrices(rate),
      message: currency === 'INR'
        ? 'India pricing shown in INR.'
        : `Approx. ${currency} pricing shown for your region.`,
      approximate: currency !== 'INR',
    });
  } catch (error) {
    const fallbackCurrency = country === 'IN' ? 'INR' : 'USD';
    const fallbackRate = FALLBACK_RATES[fallbackCurrency] || 1;

    res.status(200).json({
      country,
      currency: fallbackCurrency,
      locale: toLocale(country, fallbackCurrency),
      prices: buildPrices(fallbackRate),
      message: fallbackCurrency === 'INR'
        ? 'India pricing shown in INR.'
        : `Approx. ${fallbackCurrency} pricing shown for your region.`,
      approximate: fallbackCurrency !== 'INR',
    });
  }
};

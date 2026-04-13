// RETTELSE AF groupByYear
function groupByYear(data, valueKey) {
  if (!data || data.length === 0) return { years: [], byMonth: [] };
  const currentYear = new Date().getFullYear();
  const years = [...new Set(data.map(d => d.year || parseInt(String(d.month).split('-')[0])))].sort();
  
  const byMonth = MONTH_NAMES.map((name, i) => {
    const row = { month: name };
    const monthNum = i + 1;
    
    years.forEach(year => {
      // Vi tjekker både om d.month er et tal (4) eller en streng ("2026-04")
      const found = data.find(d => {
        const dMonth = String(d.month).includes('-') ? parseInt(d.month.split('-')[1]) : d.month;
        const dYear = d.year || parseInt(d.month.split('-')[0]);
        return dYear === year && dMonth === monthNum;
      });
      row[year] = found ? found[valueKey] : null;
    });

    const historicVals = years
      .filter(y => y < currentYear)
      .map(y => row[y])
      .filter(v => v !== null && v > 0);
    
    row["Median"] = calcMedian(historicVals);
    return row;
  });
  return { years, byMonth };
}

// RETTELSE AF DKPrices (for at håndtere "2026-04" formatet)
function DKPrices({ area }) {
  const [data, setData] = useState([]);
  useEffect(() => {
    fetch(`${API}/dk-prices/${area}`).then(r => r.json()).then(setData);
  }, [area]);

  const chartData = data.map(d => {
    // Hvis month er "2026-04", lav det om til "Apr"
    const mIdx = String(d.month).includes('-') ? parseInt(d.month.split('-')[1]) - 1 : d.month - 1;
    return { 
      month: MONTH_NAMES[mIdx], 
      Spotpris: d.spot_price, 
      Solar: d.solar_weighted, 
      Offshore: d.offshore_weighted, 
      Onshore: d.onshore_weighted 
    };
  });

  // ... resten af din DKPrices kode
}

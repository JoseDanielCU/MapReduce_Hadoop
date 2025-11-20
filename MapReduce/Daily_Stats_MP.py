from mrjob.job import MRJob
import csv
from statistics import mean, median, pstdev

class StatisticalAnalysis(MRJob):

    def configure_args(self):
        super().configure_args()
        self.add_passthru_arg("--start", help="Fecha inicio YYYY-MM-DD", default="2022-01-01")
        self.add_passthru_arg("--end", help="Fecha fin YYYY-MM-DD", default="2022-12-31")

    def mapper(self, _, line):
        try:
            row = next(csv.reader([line]))

            if row[0] == "time":
                return

            fecha = row[0].split("T")[0]

            if not (self.options.start <= fecha <= self.options.end):
                return

            temp = float(row[1])
            hum = float(row[2])
            precip = float(row[4])
            pressure = float(row[7])

            yield "stats", (temp, hum, precip, pressure)

        except:
            pass

    def reducer(self, _, values):
        temps = []
        hums = []
        precips = []
        pressures = []

        for t, h, p, pr in values:
            temps.append(t)
            hums.append(h)
            precips.append(p)
            pressures.append(pr)

        yield "global_statistics", {

            "temp_min": min(temps),
            "temp_max": max(temps),
            "temp_avg": mean(temps),
            "temp_median": median(temps),
            "temp_std_dev": pstdev(temps),

            "hum_min": min(hums),
            "hum_max": max(hums),
            "hum_avg": mean(hums),

            "precip_total": sum(precips),
            "precip_avg_hourly": mean(precips),

            "pressure_min": min(pressures),
            "pressure_max": max(pressures),
            "pressure_avg": mean(pressures),
        }


if __name__ == "__main__":
    StatisticalAnalysis.run()

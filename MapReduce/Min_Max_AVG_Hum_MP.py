from mrjob.job import MRJob
import csv

class HumidityMinMaxAVG(MRJob):
    def configure_args(self):
        super().configure_args()
        self.add_passthru_arg("--start", help="Fecha inicio YYYY-MM-DD", default="2022-01-01")
        self.add_passthru_arg("--end", help="Fecha fin YYYY-MM-DD", default="2023-01-01")

    def mapper(self, _, line):
        try:
            row = next(csv.reader([line]))
            if row[0] == "time":
                return

            fecha = row[0].split("T")[0]

            humedad = float(row[2])

            if self.options.start <= fecha <= self.options.end:
                yield fecha, humedad

        except:
            pass

    def reducer(self, fecha, hums):
        hum_list = list(hums)
        yield fecha, {"min": min(hum_list), "max": max(hum_list),"AVG": sum(hum_list)/len(hum_list)}


if __name__ == "__main__":
    HumidityMinMaxAVG.run()

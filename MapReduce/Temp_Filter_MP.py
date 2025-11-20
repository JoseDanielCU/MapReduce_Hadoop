from mrjob.job import MRJob
import csv

class FilterTemp(MRJob):

    def configure_args(self):
        super().configure_args()
        self.add_passthru_arg("--temp", type=float, default=30,help="Temperatura umbral")
        self.add_passthru_arg("--start", help="Fecha inicio YYYY-MM-DD", default="2022-01-01")
        self.add_passthru_arg("--end", help="Fecha fin YYYY-MM-DD", default="2023-01-01")

    def mapper(self, _, line):
        try:
            row = next(csv.reader([line]))
            if row[0] == "time":
                return

            fecha = row[0].split("T")[0]
            temp = float(row[1])

            if self.options.start <= fecha <= self.options.end:
                if temp > self.options.temp:
                    yield fecha, 1

        except:
            pass

    def reducer(self, fecha, values):
        yield fecha, sum(values)


if __name__ == "__main__":
    FilterTemp.run()
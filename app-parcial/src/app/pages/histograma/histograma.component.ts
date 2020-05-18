import { Component, OnInit } from '@angular/core';
import { NorthwindService } from 'src/app/services/northwind.service';
import { Label } from 'ng2-charts';
import { Observable } from 'rxjs';
import { ChartOptions, ChartType, ChartDataSets } from 'chart.js';

@Component({
  selector: 'app-histograma',
  templateUrl: './histograma.component.html',
  styleUrls: ['./histograma.component.scss']
})
export class HistogramaComponent implements OnInit {

  constructor(private north:NorthwindService) { }

  dataMes:Label[]=[];
  dataValues:ChartDataSets[]=[{data:[],label:''}];

  defaultBindingsList = [
    { value: 1, label: 'Cliente' },
    { value: 2, label: 'Producto' },
    { value: 3, label: 'Empleado'}
  ];
  selectedDimension = null;

  customer$: Observable<any>;
  year$: Observable<any>;
  month$: Observable<any>;

  selectedCustomer: string[] = [];
  selectedYear: string[] = [];
  selectedMonth: string[] = [];

  selectedParams: any = {
    clients: [],
    years: [],
    months: [],
  };


  ngOnInit(): void {
    this.selectedDimension = this.defaultBindingsList[0];
    this.customer$ = this.north.getItemsDimension(`${this.selectedDimension.label}`);

    this.year$ = this.north.getAnios();
    this.month$ = this.north.getMeses();

    this.onUpdate();
  }

  onChangeDimension($event){
    this.selectedDimension = $event;
    this.customer$ = this.north.getItemsDimension(`${this.selectedDimension.label}`);

    this.onUpdate();
  }

  onChangeCliente($event){
    this.selectedParams.clients = $event;
    this.onUpdate();
  }
  onChangeAnio($event){
    this.selectedParams.years = $event;
    this.onUpdate();
  }
  onChangeMes($event){
    this.selectedParams.months = $event;
    this.onUpdate();
  }


  onUpdate(){
    console.log(this.selectedParams);

    const {clients,years,months} = this.selectedParams;
    let body = {clients,years,months};
    for(const item in body){
      if(body[item].lenght == 0)
        body[item] = [''];
    }
    console.log(body);
    this.north.GetDataByGenerico(this.selectedDimension.label,body).subscribe((result:any)=>{

      const labels = result.datosTabla.map(label => `${label.meses} ${label.años}`).filter((item, index, arr) => arr.indexOf(item) === index);

      let graphicValues: number[] = [];
      let values = result.datosTabla.map((value, index, arr) => {
        if (index === 0) {
          graphicValues.push(value.valor);
        } else if (value.descripcion === arr[index - 1].descripcion) {
          graphicValues.push(value.valor);
        }

        if (arr[index + 1] !== undefined && value.descripcion !== arr[index + 1].descripcion) {
          const vals = graphicValues;
          graphicValues = [];
          return { label: value.descripcion, data: vals }
        } else if (arr[index + 1] === undefined) {
          if (graphicValues.length === 0) graphicValues.push(value.valor);
          return { label: value.descripcion, data: graphicValues }
        }
        else return undefined;
      });

      values = values.filter(v => v !== undefined);

      const graphicData: ChartDataSets[] = values;
      const emptyData: ChartDataSets[] = [{ label: '', data: [0] }];


      this.dataMes = labels ? labels : '';
      this.dataValues = graphicData.length !== 0 ? graphicData : emptyData;
    });
  }
}

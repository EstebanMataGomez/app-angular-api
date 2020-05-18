import { Component, OnInit } from '@angular/core';
import { NorthwindService } from 'src/app/services/northwind.service';
import { Label } from 'ng2-charts';
import { Observable } from 'rxjs';

@Component({
  selector: 'app-top5',
  templateUrl: './top5.component.html',
  styleUrls: ['./top5.component.scss']
})
export class Top5Component implements OnInit {

  constructor(private north:NorthwindService) { }

  //Variables de datos
  dataDimension:Label[]=[];
  dataValues:number[]=[];
  //NG select
  defaultBindingsList = [
    { value: 1, label: 'Cliente' },
    { value: 2, label: 'Producto' },
    { value: 3, label: 'Empleado'}
  ];
  selectedDimension = null;
  //NG other select
  customer$: Observable<any>;
  selectedCustomer: string[] = [];
  year$: Observable<any>;
  selectedYear: string[] = [];
  years: string[] = [];
  month$: Observable<any>;
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
  }

  onChangeDimension($event){
    this.selectedDimension = $event;

    this.customer$ = this.north.getItemsDimension(`${this.selectedDimension.label}`);
  }
  
  onChangeClient($event){
    this.selectedParams.clients = $event;
    this.onUpdate();
    //this.north.getDataGeneric(this.selectedDimension.label, this.selectedCustomer, this.selectedMonth, this.selectedYear).subscribe((result:any)=>{
    //  this.dataDimension = result.datosDimension;
    //  this.dataValues = result.datosVenta
    //});
  }
  onChangeYear($event){
    this.selectedParams.years = $event;
    this.onUpdate();
  }
  onChangeMonth($event){
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
        this.dataDimension = result.datosDimension;
        this.dataValues = result.datosVenta;
    });
  }

  
}

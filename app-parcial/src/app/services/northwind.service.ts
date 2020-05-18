import { Injectable } from '@angular/core';
import { HttpClient } from '@angular/common/http';
import { environment } from 'src/environments/environment';
import { map } from 'rxjs/operators';

const urlAPI = environment.API.EndPoint.Northwind;

@Injectable({
  providedIn: 'root'
})
export class NorthwindService {

  constructor(private http:HttpClient) { 

  }

  getTop5(dimension:string, orden:string){
    return this.http.get(`${urlAPI}Top5/${dimension}/${orden}`);
  }

  getSerieHistorica(){

  }

  getItemsDimension(dimension:string){
    return this.http.get(`${urlAPI}GetItemByDimension/${dimension}`).pipe(
      map((result:any)=>result.datosDimension)
    );
  }

  getDataPieDimension(dimension:string,values:string[]){
    return this.http.post(`${urlAPI}GetDataPieByDimension/${dimension}`, values).pipe(
      map((result:any)=>result)
    );
  }

  getDataGeneric(dimension:string,values:string[],meses:string[],anios:string[]){
    return this.http.post(`${urlAPI}GetDataGeneric/${dimension}/[${anios}]/[${meses}]`, values).pipe(
      map((result:any)=>result)
    );
  }

  getAnios(){
    return this.http.get(`./assets/anios.json`);
  }

  getMeses(){
    return this.http.get(`./assets/meses.json`);
  }

  GetDataByGenerico(dim: string, values: any) {
    const dimension: string = `[Dim%20${dim}].[Dim%20${dim}%20Nombre]`;
    return this.http.post(`${urlAPI}GetDataByGenerico/${dimension}/DESC`, values).pipe(
        map((result: any) => result));
  }

}

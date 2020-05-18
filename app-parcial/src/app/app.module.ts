import { BrowserModule } from '@angular/platform-browser';
import { NgModule } from '@angular/core';

import { AppRoutingModule } from './app-routing.module';
import { AppComponent } from './app.component';
import { InicioComponent } from './pages/inicio/inicio.component';
import { AcercaDeComponent } from './pages/acerca-de/acerca-de.component';
import { Top5Component } from './pages/top5/top5.component';
import { HistogramaComponent } from './pages/histograma/histograma.component';
import { NavbarComponent } from './components/navigation/navbar/navbar.component';
import { ChartsModule } from 'ng2-charts';
import { LineChartComponent } from './components/charts/line-chart/line-chart.component';
import { BarChartComponent } from './components/charts/bar-chart/bar-chart.component';
import { PieChartComponent } from './components/charts/pie-chart/pie-chart.component';
import { HttpClientModule } from '@angular/common/http';
import { NgSelectModule } from '@ng-select/ng-select';
import { FormsModule } from '@angular/forms';

@NgModule({
  declarations: [
    AppComponent,
    InicioComponent,
    AcercaDeComponent,
    Top5Component,
    HistogramaComponent,
    NavbarComponent,
    LineChartComponent,
    BarChartComponent,
    PieChartComponent
  ],
  imports: [
    BrowserModule,
    AppRoutingModule,
    ChartsModule,
    HttpClientModule,
    NgSelectModule,
    FormsModule
  ],
  providers: [],
  bootstrap: [AppComponent]
})
export class AppModule { }

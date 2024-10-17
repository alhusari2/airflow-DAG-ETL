import pandas as pd
import warnings
import gspread

warnings.simplefilter(action='ignore', category=FutureWarning)

class DataCleaner:
    def __init__(self, service_account_file, spreadsheet_name_kc, spreadsheet_name_kcp):
        # Initialize connection to Google Sheets
        self.gc = gspread.service_account(filename=service_account_file)
        self.spreadsheet_name_kc = spreadsheet_name_kc
        self.spreadsheet_name_kcp = spreadsheet_name_kcp

    def cleaning_data_KC(self):
        all_cleaned_data = []
        try:
            # Connect to the KC spreadsheet
            spreadsheet = self.gc.open(self.spreadsheet_name_kc)
            worksheets = spreadsheet.worksheets()
            
            for sheet in worksheets[:5]:
                sheet_name = sheet.title
                if sheet_name == "KANCA":
                    continue
                
                # Read all values from the sheet
                worksheets = spreadsheet.worksheet(sheet_name)
                all_values = worksheets.get_all_values()
                df = pd.DataFrame(all_values)

                # Preprocess the data (you can modularize this if needed)
                data = df.iloc[8:, 2:19]
                data.drop(data.index[-1], inplace=True)  # Remove last row (Total)
                
                # Process columns 3 and 4
                data_check = data[[3, 4]]
                data_check.loc[data_check[3].notna(), 3] = data_check.loc[data_check[3].notna(), 4]
                data_check[3].fillna(method='ffill', inplace=True)
                data[[3, 4]] = data_check
                data.reset_index(drop=True, inplace=True)
                
                # Select relevant columns for the cleaned dataset
                data_copy = data[[2, 3, 4, 5, 6, 9, 16, 18]]
                columns = ['Sandi RKA', 'Header KPI', 'Item KPI', 'Bobot', 'Target', 'Realisasi', 'Pencapaian', 'Skor']
                data_copy.columns = columns
                list_kpi = ['FBI', 'LAR', 'Merchant']
                data_copy['Bobot'] = data_copy.apply(lambda row: 0 if row['Item KPI'] in list_kpi else row['Bobot'], axis=1)
                data_copy.fillna(0, inplace=True)
                
                # Add metadata columns
                data_copy["Main Branch"] = f"KC {sheet_name}"
                data_copy["Unit Kerja"] = f"KC {sheet_name}"
                data_copy["Periode"] = "31/07/2023"

                # Clean underscores and spaces
                for col in ['Unit Kerja', 'Main Branch']:
                    data_copy[col] = data_copy[col].str.replace('_', ' ').str.strip()

                data_copy.reset_index(drop=True, inplace=True)
                all_cleaned_data.append(data_copy)

                print(f"Sheet '{sheet_name}' berhasil diproses.")
            
            # Combine all the cleaned data
            if all_cleaned_data:
                combined_data_KC = pd.concat(all_cleaned_data, ignore_index=True)
                return combined_data_KC
            
            else:
                print("Tidak ada data yang berhasil dibersihkan untuk digabungkan.")
                return None

        except Exception as e:
            print(f"Terjadi kesalahan saat memproses file: {e}")
            return None

    def cleaning_data_KCP(self):
        combined_data_KCP = []
        try:
            # Connect to the KCP spreadsheet
            spreadsheet = self.gc.open(self.spreadsheet_name_kcp)
            worksheets = spreadsheet.worksheets()

            for sheet in worksheets[:5]:
                sheet_name = sheet.title
                if sheet_name == "KANCA":
                    continue
                
                # Read all values from the sheet
                worksheets = spreadsheet.worksheet(sheet_name)
                all_values = worksheets.get_all_values()
                df = pd.DataFrame(all_values)

                # Preprocess the data
                data = df.iloc[8:, 2:19]
                data.drop(data.index[-1], inplace=True)
                data_check = data[[3, 4]]
                data_check.loc[data_check[3].notna(), 3] = data_check.loc[data_check[3].notna(), 4]
                data_check[3].fillna(method='ffill', inplace=True)
                data[[3, 4]] = data_check
                data.reset_index(drop=True, inplace=True)

                # Select relevant columns
                data_copy = data[[2, 3, 4, 5, 6, 9, 16, 18]]
                columns = ['Sandi RKA', 'Header KPI', 'Item KPI', 'Bobot', 'Target', 'Realisasi', 'Pencapaian', 'Skor']
                data_copy.columns = columns
                data_copy["Unit Kerja"] = f"KCP {sheet_name}"
                data_copy['Main Branch'] = data_copy['Unit Kerja'].apply(self.find_kc)
                data_copy["Periode"] = "31/07/2023"
                
                # Apply transformations
                list_kpi = ['FBI', 'LAR', 'Merchant']
                data_copy['Bobot'] = data_copy.apply(lambda row: 0 if row['Item KPI'] in list_kpi else row['Bobot'], axis=1)
                data_copy.fillna(0, inplace=True)

                # Clean underscores and spaces
                for col in ['Unit Kerja', 'Main Branch']:
                    data_copy[col] = data_copy[col].str.replace('_', ' ').str.strip()

                combined_data_KCP.append(data_copy)

                print(f"Sheet '{sheet_name}' berhasil diproses.")
            
            if combined_data_KCP:
                combined_data_KCP = pd.concat(combined_data_KCP, ignore_index=True)
                return combined_data_KCP
            else:
                print("Tidak ada data yang berhasil dibersihkan untuk digabungkan.")
                return None

        except Exception as e:
            print(f"Terjadi kesalahan saat memproses file: {e}")
            return None

    def find_kc(self, nama_kcp):
        # Note penggunaan spasi diganti menjadi _
        kc_to_kcp = {
                "KC JAKARTA KCK": ["KC JAKARTA KCK"],
                "KC JAKARTA KELAPA_GADING": ["KC JAKARTA KELAPA_GADING"],
                "KC JAKARTA ARTHA_GADING": ["KC JAKARTA ARTHA_GADING", "KCP GADING_BOULEVARD_RAYA"],
                "KC JAKARTA CEMPAKA_MAS": ["KC JAKARTA CEMPAKA_MAS", "KCP CEMPAKA_PUTIH_RAYA"],
                "KC JAKARTA CUT_MUTIAH": ["KC JAKARTA CUT_MUTIAH", "KCP CIKINI", "KCP MENTENG"],
                "KC JAKARTA GADING_BOULEVARD": ["KC JAKARTA GADING_BOULEVARD", "KCP CAKUNG_TIPAR", "KCP GADING_BOULEVARD_TIMUR", "KCP GADING_ELOK", "KCP PENGGILINGAN"],
                "KC JAKARTA GUNUNG_SAHARI": ["KC JAKARTA GUNUNG_SAHARI", "KCP PANGERAN_JAYAKARTA"],
                "KC JAKARTA HAYAM_WURUK": ["KC JAKARTA HAYAM_WURUK", "KCP GAJAH_MADA", "KCP GLODOK", "KCP JEMBATAN_LIMA", "KCP LOKASARI_PLAZA", "KCP MANGGA_BESAR"],
                "KC JAKARTA JATINEGARA": ["KC JAKARTA JATINEGARA", "KCP KLENDER", "KCP MEESTER"],
                "KC JAKARTA KOTA": ["KC JAKARTA KOTA", "KCP PURI_DELTA_MAS", "KCP TUBAGUS_ANGKE"],
                "KC JAKARTA KRAMAT": ["KC JAKARTA KRAMAT", "KCP MANGGARAI", "KCP PRAMUKA","KCP BPKP"],
                "KC JAKARTA KREKOT": ["KC JAKARTA KREKOT", "KCP KARANGANYAR", "KCP PASAR_BARU"],
                "KC JAKARTA MANGGA_ DUA": ["KC JAKARTA MANGGA_DUA", "KCP HARCO_MANGGA_DUA", "KCP PASAR_PAGI_MANGGA_DUA"],
                "KC JAKARTA PLUIT": ["KC JAKARTA PLUIT", "KCP MUARA_ANGKE", "KCP MUARA_KARANG", "KCP TELU_GONG"],
                "KC JAKARTA RASUNA_SAID": ["KC JAKARTA RASUNA_SAID", "KCP KOTA_KASABLANKA", "KCP KUNINGAN_EPISENTRUM"],
                "KC JAKARTA ROXI": ["KC JAKARTA ROXI", "KCP KAMPUS_II_UNTAR", "KCP TOMANG"],
                "KC JAKARTA SEGITIGA_SENEN": ["KC JAKARTA SEGITIGA_SENEN","KCP RSPAD"],
                "KC JAKARTA SUDIRMAN": ["KC JAKARTA SUDIRMAN", "KCP BENDUNGAN_HILIR", "KCP THAMRIN_CITY"],
                "KC JAKARTA SUNTER": ["KC JAKARTA SUNTER", "KCP DANAU_SUNTER_UTARA", "KCP PURI_MUTIARA"],
                "KC JAKARTA TANAH_ABANG": ["KC JAKARTA TANAH_ABANG", "KCP BLOK_B", "KCP KEBON_KACANG", "KCP PASAR_TANAH_ABANG", "KCP T_PLAZA", "KCP TELUK_GONG"],
                "KC JAKARTA VETERAN": ["KC JAKARTA VETERAN","KCP TASPEN","KCP KEMENTRIAN_BUMN","KCP LEMHANAS","KCP PERTAMINA","KCP ABDUL_MUIS", "KCP DEPKEU"],
                "KC JAKARTA MALL_AMBASADOR": ["KC MALL_AMBASADOR"]
            }
        for kc, daftar_unit in kc_to_kcp.items():
            if any(nama_kcp.upper() in unit.upper() for unit in daftar_unit):
                return kc
        return "Unknown"

    def combine_and_save(self, output_path):
        hasil_KC = self.cleaning_data_KC()
        hasil_KCP = self.cleaning_data_KCP()
        if hasil_KC is not None and hasil_KCP is not None:
            combined_data = pd.concat([hasil_KC, hasil_KCP], ignore_index=True)
            combined_data.to_excel(output_path, index=False)
            print(f"Data gabungan berhasil disimpan di: {output_path}")
        else:
            print("Tidak ada data yang bisa digabungkan.")


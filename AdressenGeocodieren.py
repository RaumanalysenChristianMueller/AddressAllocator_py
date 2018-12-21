# -*- coding: utf-8 -*-

import processing
from PyQt5.QtCore import (QCoreApplication,
                          QVariant)
from qgis.core import (QgsProcessing,
                       QgsProcessingAlgorithm,
                       QgsProcessingParameterFeatureSource,
                       QgsProcessingParameterField,
                       QgsProcessingParameterFolderDestination,
                       QgsProcessingParameterBoolean,
                       QgsMessageLog,
                       QgsField,
                       QgsExpression,
                       QgsGeometry,
                       QgsPointXY,
                       QgsFeature,
                       QgsVectorLayer,
                       QgsVectorFileWriter,
                       QgsProject)
from qgis.utils import iface
import pandas as pd
import os
import urllib.request
import zipfile


class allocateAddresses(QgsProcessingAlgorithm):
    
    """
    This script geocodes addresses using official address data in NRW - Germany
    """

    inputTab = 'inputTab'
    street = 'street'
    hnr = 'hnr'
    hnrz = 'hnrz'
    ags = 'ags'
    redownload = 'redownload'
    OUTPUT = 'output'
	

    def initAlgorithm(self, config = None):
        
        # define input parameters
        self.addParameter(
            QgsProcessingParameterFeatureSource(
                self.inputTab,
                self.tr('Zu geocodierende Adressen als CSV'),
                [QgsProcessing.TypeFile]
            )
        )
        
        self.addParameter(
            QgsProcessingParameterField(
                self.street,
                self.tr('Spalte mit Straßennamen'),
                None,
                self.inputTab
            )
        )
        
        self.addParameter(
            QgsProcessingParameterField(
                self.hnr,
                self.tr('Spalte mit Hausnummern'),
                None,
                self.inputTab
            )
        )
        
        self.addParameter(
            QgsProcessingParameterField(
                self.hnrz,
                self.tr('Spalte mit Hausnummer-Zusätzen'),
                None,
                self.inputTab
            )
        )
        
        self.addParameter(
            QgsProcessingParameterField(
                self.ags,
                self.tr('Spalte mit amtlichem Gemeindeschlüssel (AGS)'),
                None,
                self.inputTab
            )
        )
        
        self.addParameter(
            QgsProcessingParameterBoolean(
                self.redownload,
                self.tr('Lade aktuelle amtliche Adressen')
            )
        )
        
        
        self.addParameter(
            QgsProcessingParameterFolderDestination(
                self.OUTPUT,
                self.tr('Ausgabe-Ordner')
            )
        )
        
        
    def processAlgorithm(self, parameters, context, feedback):
        
        # get inputs
        inputTab = self.parameterAsVectorLayer(parameters, self.inputTab, context)
        street = self.parameterAsString(parameters, self.street, context)
        hnr = self.parameterAsString(parameters, self.hnr, context)
        hnrz = self.parameterAsString(parameters, self.hnrz, context)
        ags = self.parameterAsString(parameters, self.ags, context)
        redownload = self.parameterAsString(parameters, self.redownload, context)
        outDir = self.parameterAsString(parameters, self.OUTPUT, context)
        
        allAtts = inputTab.dataProvider().fields().names()
        
                         
        # import qgis attribute table as pandas data frame
        QgsMessageLog.logMessage('Converting QGIS table to pandas data frame.', 'User notification', 0)
        atts = pd.DataFrame([], columns = allAtts)
        i = 0
        for feature in inputTab.getFeatures():
            atts.loc[i] = feature.attributes()
            i += 1
        
        
        # download official address data
        thisScript_path = os.path.dirname(os.path.realpath(__file__))
        of_path = thisScript_path + "\\gebref_EPSG4647_ASCII\\gebref.txt"
        if (os.path.isfile(of_path) == False) or (redownload == 'true'):
            QgsMessageLog.logMessage('Downloading official address dataset.', 'User notification', 0)
            url = 'https://www.opengeodata.nrw.de/produkte/geobasis/lika/alkis_sek/gebref/gebref_EPSG4647_ASCII.zip' 
            urllib.request.urlretrieve(url, thisScript_path + '\\gebref_EPSG4647_ASCII.zip')
            with open(thisScript_path + '\\gebref_EPSG4647_ASCII.zip', 'rb') as f:
                z = zipfile.ZipFile(f)
                for name in z.namelist():
                    z.extract(name, thisScript_path + '\\gebref_EPSG4647_ASCII\\')
        
        
        
        # import and prepare official address data as pandas data frame
        def loadOfficialAddresses(of_path):
            
            # load data from file
            QgsMessageLog.logMessage('Loading official address data set.', 'User notification', 0)
            of = pd.read_csv(of_path,
                             sep = ';', decimal = ',', header = None,
                             encoding = 'UTF-8')
            
            # add column names
            of.columns = list(map(str, list(range(0,14))))
            
            # build ags identifier (for consistent community identification)
            QgsMessageLog.logMessage('Building AGS identifier.', 'User notification', 0)
            of['agsField'] = of['3'].apply(str).str.cat(of['4'].apply(str))
            of['agsField'] = of['agsField'].str.cat(of['5'].apply(str))
            
            # add prefix zeros for community number consistency
            temp_str = of['6'].apply(str)
            cond = temp_str.apply(len) == 1
            temp_str.loc[cond] = '00' + temp_str.loc[cond]
            cond = temp_str.apply(len) == 2
            temp_str.loc[cond] = '0' + temp_str.loc[cond]
            
            # add ags identifier to data frame
            of['agsField'] = of['agsField'].str.cat(temp_str)
            
            return of
        
        of = loadOfficialAddresses(of_path)
        
        # define function for building a key addressID field
        def buildAddressID(inTable = atts, streetField = street,
                           housenumberField = hnr, housenumberappendixField = hnrz,
                           agsField = ags):
       
            outTable = inTable
            
                       
            # build address ID
            outTable['addressID'] = outTable[streetField].str.cat(outTable[housenumberField].apply(str), sep = "")
            outTable['addressID'] = outTable['addressID'].str.cat(outTable[housenumberappendixField].apply(str), sep = "")
            outTable['addressID'] = outTable['addressID'].str.cat(outTable[agsField].apply(str), sep = "_")
            
            # replace characters for key field consistency
            outTable['addressID'] = outTable['addressID'].str.lower()
            outTable['addressID'] = outTable['addressID'].str.replace('null', '')
            outTable['addressID'] = outTable['addressID'].str.replace('nan', '')
            outTable['addressID'] = outTable['addressID'].str.replace(' ', '')
            outTable['addressID'] = outTable['addressID'].str.replace('-', '')
            outTable['addressID'] = outTable['addressID'].str.replace('ß', 'ss')
            outTable['addressID'] = outTable['addressID'].str.replace('ü', 'ue')
            outTable['addressID'] = outTable['addressID'].str.replace('ö', 'oe')
            outTable['addressID'] = outTable['addressID'].str.replace('ä', 'ae')
            outTable['addressID'] = outTable['addressID'].str.replace('str.', 'str')
            outTable['addressID'] = outTable['addressID'].str.replace('strße', 'str')
            outTable['addressID'] = outTable['addressID'].str.replace('strasse', 'str')
            outTable['addressID'] = outTable['addressID'].str.replace('straße', 'str')
            outTable['addressID'] = outTable['addressID'].str.replace('strsse', 'str')
            outTable['addressID'] = outTable['addressID'].str.replace('.', '')
    
            return outTable
                    
        
        
        # build addressID fields
        QgsMessageLog.logMessage('Building key address ID field for spreadsheet address data.', 'User notification', 0)
        atts = buildAddressID()
        QgsMessageLog.logMessage('Building key address ID field for official address data set.', 'User notification', 0)
        of = buildAddressID(inTable = of, streetField = '13',
                            housenumberField = '9', housenumberappendixField = '10',
                            agsField = 'agsField')
        
        
        
        
        
        # join tables
        QgsMessageLog.logMessage('Joining data sets.', 'User notification', 0)
        joinTab = atts.join(of.set_index('addressID'), on = 'addressID')
        
        # extract matched and not matched addresses
        QgsMessageLog.logMessage('Writing joined data sets to file.', 'User notification', 0)
        missingAdds = joinTab[joinTab['11'].isnull()]
        joinTab = joinTab[joinTab['11'].notnull()]
        missingAdds.to_csv(outDir + '/fehlendeAdressen_nichtInAmtlichenAdressverzeichnis.csv', index = False, encoding = 'ANSI')
        joinTab.to_csv(outDir + '/abgeglicheneAdressen_imAmtlichenAdressverzeichnis.csv', index = False, encoding = 'ANSI')
       
        # add key id for joining to original data table
        atts['keyID'] = list(range(0, len(atts.index)))
        atts.to_csv(outDir + '/inTabelle_mitSchluessel.csv', index = False, encoding = 'ANSI')
        
        
        # convert coordinates to points
        def convertCoordinatesToPoints(inTable = joinTab):
             
            QgsMessageLog.logMessage('Converting XY coordinates to spatial points.', 'User notification', 0)
            
            # create temporary vector layer
            tempLyr = QgsVectorLayer('Point', 'temporary_points', 'memory')
            
            # define coordinate system
            CRS = tempLyr.crs()
            CRS.createFromId(4647)
            tempLyr.setCrs(CRS)
             
            # start editing
            tempLyr.startEditing()
            
            # create attribute fields
            for f in list(range(0, len(atts.columns))):
                tempLyr.dataProvider().addAttributes([QgsField(atts.columns[f], QVariant.String)])
            tempLyr.updateFields()
            
            # iterate over each address and add geometry
            for p in list(range(0, len(joinTab['11']))):
                 
                QgsMessageLog.logMessage('Creating features...(' + str(p) + '/' + str(len(joinTab['11'])) + ')', 'User notification', 0)
                
                # extract coordinates
                x = joinTab['11'].iloc[p]
                y = joinTab['12'].iloc[p]
                
                 
                # convert coordinates to geometry
                pGeom = QgsGeometry.fromPointXY(QgsPointXY(x, y))
                 
                # create feature
                feature = QgsFeature()
                 
                # set coordinates for the new feature
                feature.setGeometry(pGeom)
                
                # add attributes
                feature.setAttributes(joinTab.values.tolist()[p])
                
                # add feature to temporary layer
                tempLyr.dataProvider().addFeature(feature)
                 
                # update the extent of the layer
                tempLyr.updateExtents()
             
            
         
            # end editing
            tempLyr.commitChanges()
            
            
            # write temporary layer to file
            QgsMessageLog.logMessage('Wrting results to file.', 'User notification', 0)
            outPath = outDir + '\\geocodierteAdressen_imAmtlichenAdressverzeichnis.gpkg'
            QgsVectorFileWriter.writeAsVectorFormat(tempLyr, outPath, 'ANSI', tempLyr.crs(), 'GPKG')
            
            return tempLyr
        
                    
        # execute function
        outLyr = convertCoordinatesToPoints()
                    
        return {}

    def name(self):
        return 'allocateAddresses'

    def displayName(self):
        return self.tr('Mit amtlichen Adressen geocodieren')

    def group(self):
        return self.tr('Raumanalaysen - Christian Mueller')

    def groupId(self):
        return 'Raumanalysen'

    def tr(self, string):
        return QCoreApplication.translate('Processing', string)

    def createInstance(self):
        return allocateAddresses()

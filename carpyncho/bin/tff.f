c
c**************************************************************************
c             THIS CODE PERFORMS TEMPLATE LIGHT CURVE FITTING             *
c**************************************************************************
c                                 
c  Version:   December 04, 2006
c  ~~~~~~~~
c
c  Input:   template.dat  -- Fourier decompositions of the template
c  ~~~~~~                    light curves
c
c             target.lis  -- List of targets
c
c                tff.par  -- Input parameter list
c
c 
c  Output:       tff.dat  -- Result of the Template Fourier Fitting  
c  ~~~~~~~
c                dff.dat  -- Result of the Direct Fourier Fitting 
c
c              match.dat  -- List of target/template matches
c
c
c  Remarks:  - The best template is selected on the basis of 
c  ~~~~~~~~    the best (i.e., minimum)  normalized fitting 
c              accuracy, defined as sigma/dsqrt(n), where 'sigma' 
c              is the standard deviation between the target and 
c              the template, 'n' is the number of target data 
c              points after outlier selection. 
c              [Currently, outlier selection is recommended with 
c              some 'caution' ... - i.e., in the case of low number 
c              of data points, solution might converge to a wrong 
c              template.] 
c
c            - Results (best template Fourier decomposition, 
c              *tff.dat*, matching target/template names and 
c              periods, *match.dat*, direct Fourier decompositions, 
c              *dff.dat*) are printed out by deleting the former 
c              contents of the output files.
c
c            - Only those template members are used which satisfy 
c              the following  'N'  and  'SNR1'  criteria: 
c
c              N    > N_min
c              SNR1 > SNR1_min   [ SNR1=A(1)/(sigma/sqrt(N)) ] 
c
c              [see *tff.par* for the actual values of these parameters]
c
c            - The direct Fourier fit is performed with the optimum 
c              order as defined in routine  'dfdeco'
c
c            - If the number of data points of the target light curve  
c              is lower than 'mindp', then the object is skipped.
c
c            - If the input parameter  jfit < 0, then the order M of the 
c              polynomial template fit is chosen as follows: 
c
c              M = 0,   if   0.0 < SNR <  50.0
c              M = 1,   if  50.0 < SNR < 150.0 
c              M = 2,   if 150.0 < SNR 
c
c              Here  SNR=AMP/(sigma/sqrt(n)), where 'AMP' is the total 
c              amplitude of the best fitting template, 'sigma' is the 
c              standard deviation of the residuals between the target 
c              and the template, 'n' is the number of the target data 
c              points. SNR is computed by using the template fit with 
c              M=1.
c
c            - The array dimensions and output format are set to fit 
c              up to 15 Fourier components. For changing this limit 
c              one has to change 'related' stuff ...
c
c            - Maximum array dimensions are set as follows:      
c
c              maxdim = 3456 ... Maximum number of the data points of the 
c                                input time series.
c              maxdi1 = 843  ... Maximum number of targets (or templates). 
c                                Maximum number of bins in the synthetic 
c                                template light curves. 
c
c              These limits can be changed with some caution, by replacing 
c              the above numbers by some other ones, not appearing as   
c              labels or other constants in this code. This is admittedly 
c              a primitive solution of dimension changing, but works 
c              securely. 
c
c            - Input time series is read in by subroutine 'read1'. 
c              It is assumed that the time series is stored sequentially 
c              as {time(i),value(i)}.
c
c==========================================================================
c
      implicit real*8 (a-h,o-z)
c
      character*22 tstar,sname(843)
      character*22 sn(843)
      character*70 path(843)
      character*70 fname
c
      dimension pert(843)
      dimension per(843),a0(843)
      dimension amp(20,843),pha(20,843),sig(843),era(843)
c	
      dimension am(20),ph(20),c(11)
      dimension damp(20),dpha(20)
      dimension vt(843,843),dev(843),erm(843)
      dimension tl(3456),vl(3456)
      dimension tin(3456),xin(3456),ts(3456),xs(3456)
      dimension x1(3456),y1(3456),y2(3456)
      dimension num(843),itop(843)
c
c==========================================================================
c
c...  Read in basic parameters
c
      call tffpar(ntbin,nmin,mindp,snr1min,nmatch,dph,asig,jfit)
c
c...  Setting maximum dimensions 
c     (as they appear in the 'dimension' statements)
c 
      maxdim  = 3456
      maxdi1  = 843
c
c...  Wired-in parameters
c
      mord    = 15
      inph    = 1
      iq      = 1
      ref31   = 5.1
      iopt    = 0
c
c...  Parameter-dependent changes
c
      if(ntbin.gt.maxdi1) ntbin = maxdi1
      if(jfit.lt.0)       iopt  = 1
      jfit0=jfit
c
c==========================================================================
c     >>>> WIRED-IN parameters:
c
c     maxdim= Maximum number of the data points of the input time series  
c             (as it is defined in the 'dimension' declaration) 
c
c     maxdi1= Maximum number of templates and maximum number of targets
c             (as it is defined in the 'dimension' declaration) 
c             Maximum number of samples (bins) in the synthetic 
c             template light curves 
c             (as it is defined in the 'dimension' declaration) 
c
c     mord  = Order of the Fourier sum used in the generation of the 
c             synthetic template time series
c
c     inph  = 1 --- start phased template time series at the maximum 
c                   brightness
c             2 --- start phased template time series at the minimum 
c                   brightness
c
c     iq    = 0 -- For no quadratic interpolation in the computation
c                  of the template time series at the moments of the 
c                  target time series
c             1 -- For    quadratic interpolation in the computation
c                  of the template time series at the moments of the 
c                  target time series
c      
c     ref31 = Average value of 'phi_31' in the case of RR Lyrae stars
c
c     iopt  = 0 -- Perform non-optimized TFF analysis by using the 
c                  template polynomial order as given in the parameter 
c                  file by the parameter 'jfit' (jfit>0 or jfit=0)
c
c             1 -- Perform optimized TFF analysis (jfit<0)
c
c--------------------------------------------------------------------------
c     >>>> INPUT parameters:
c
c     ntbin  = Number of bins used in the computation of the template 
c              light curves
c
c     nmin   = Template light curves with number of data points < nmin 
c              are omitted
c
c     mindp  = Target   light curves with number of data points < mindp 
c              are skipped
c
c     snr1min= Template light curves with  SNR < snr1min  are omitted 
c              [ SNR = A(1)/(sigma/sqrt(N)) ]
c
c     nmatch = First  *nmatch*  best matches are printed out 
c
c     dph    = Template time series are fitted with an accuracy of 
c              *dph*  in phase
c
c     asig   = Data points deviating more than  asig*sigma  in the  
c              search for the best-fitting template, or computing DFF, 
c              are omitted.  
c
c     jfit   = Template polynomial degree,         if  jfit > -1
c              Optimized polynomial degree is used if  jfit <  0
c==========================================================================
c
c...  Open output data files
c
      open(7,file='tff.dat')
      open(8,file='dff.dat')
      open(9,file='match.dat')
c
c...  Read in Fourier decompositions of the template light curves
c
      call fourin(maxdi1,nmin,snr1min,ntemp,sname,per,a0,
     &amp,pha,sig)
c
c...  Compute template light curves
c
      call tlc(inph,ntbin,mord,ntemp,per,amp,pha,vt)
c
c...  Read in target names, periods and paths
c
      call filenamein3(sn,path,pert,ntarg)
c
      write(*,101) ntarg,ntemp
 101  format(/,'    Number of target light curves = ',i4,/,
     &         '  Number of template light curves = ',i4,/)
c
c*******************************************************************
c     Start computing template fits for the target light curves    *
c*******************************************************************
c
      write(*,102)
 102  format(/,'WORKING !!',/)
c
      do 100 it=1,ntarg
c
      write(*,*) it,ntarg
c
      itar=it
c
c...  Read in target time series
c
      ermin  = 0.0
      t00    = 0.0
      fname  = path(it)
      tstar  = sn(it)
      period = pert(it)
      call read1(fname,nt,tl,vl)
c
      if(nt.lt.mindp) go to 100
c
      jfit1=1
      if(iopt.eq.0) go to 820
c
c     Compute optimum polynomial order
c
      jfit=1      
c
      call bestfit(itest,period,tstar,nt,tl,vl,
     &iq,jfit,asig,ntbin,dph,ntemp,sname,vt,
     &dev,era,num,nm,x1,y1,y2,devmin,epoch0,c,kmin)
c
      call minmax(nm,y2,ymin,ymax)
      ampli=ymax-ymin
      ermin=devmin/dsqrt(dfloat(nm))
      snrat1=ampli/ermin
      if(snrat1.gt.999.9) snrat1=999.9 
c
      if(snrat1.lt.50.0)  jfit1=0
      if(snrat1.gt.150.0) jfit1=2 
c
 820  continue
c
      if(iopt.eq.0) jfit=jfit0
      if(iopt.eq.1) jfit=jfit1
c
c...  Find the best template fit to the current target
c
      call bestfit(itest,period,tstar,nt,tl,vl,
     &iq,jfit,asig,ntbin,dph,ntemp,sname,vt,
     &dev,era,num,nm,x1,y1,y2,devmin,epoch0,c,kmin)
c
      ermin   = devmin/dsqrt(dfloat(nm))
      erm(it) = ermin
c
c...  Compute Fourier decomposition of the best fitting template curve 
c
      call bestfour(jfit,c,vt,ntbin,kmin,
     &period,mord,amp,pha,a0b,am,ph,p31)
c
c...  Compute DIRECT FOURIER FIT
c
      nin=nt
      do 320 i=1,nin
      tin(i)=tl(i)
      xin(i)=vl(i)
 320  continue
      call dfdeco(asig,t00,period,nin,tin,xin,
     &nsynt,ts,xs,dsigma,a00,damp,dpha,mopt)
c
c*************************************************
c     Print out decomposition obtained by TFF    *
c*************************************************
c
      iclus=1
      write(7,1200) tstar
      write(7,826)  period,t00,a0b,nm,devmin
      write(7,814)  am(1),ph(1),am(2),ph(2),am(3),ph(3),
     &am(4),ph(4),am(5),ph(5)
      write(7,814)  am(6),ph(6),am(7),ph(7),am(8),ph(8),
     &am(9),ph(9),am(10),ph(10)
      write(7,814)  am(11),ph(11),am(12),ph(12),am(13),ph(13),
     &am(14),ph(14),am(15),ph(15)
c
c*************************************************
c     Print out decomposition obtained by DFF    *
c*************************************************
c
      write(8,1200) tstar
      write(8,826)  period,t00,a00,nin,dsigma
      write(8,814)  damp(1),dpha(1),damp(2),dpha(2),damp(3),dpha(3),
     &damp(4),dpha(4),damp(5),dpha(5)
      write(8,814)  damp(6),dpha(6),damp(7),dpha(7),damp(8),dpha(8),
     &damp(9),dpha(9),damp(10),dpha(10)
      write(8,814)  damp(11),dpha(11),damp(12),dpha(12),
     &damp(13),dpha(13),damp(14),dpha(14),damp(15),dpha(15)
 1200 format(a22)   
 814  format(10(1x,f7.4))
 826  format(1x,f13.10,3x,f7.4,1x,f7.3,2x,i6,2x,f6.4)
c
 322  continue
c
c-------------------------------------------------------------------
c...  Print out top  *nmatch*  matches
c
      call topmatch(ntemp,nmatch,era,itop)
c
      write(9,303) tstar,period,devmin,jfit,snrat1
      do 302 j=1,nmatch
      i=itop(j)
      f31 = pha(3,i)-3.0*pha(1,i)
      call phrange(f31,ref31)
      write(9,303) sname(i),per(i),dev(i),num(i),f31
 303  format(a22,f9.5,1x,f7.4,1x,i4,1x,f10.4)
 302  continue
      write(9,*) 
c
c-------------------------------------------------------------------
c
 100  continue 
c
      close(7)
      close(8)      
      close(9)
c
      stop
      end
c
c
      subroutine tffpar(ntbin,nmin,mindp,snr1min,nmatch,dph,asig,jfit)
c
c-----------------------------------------------------------
c     This routine reads in basic parameters for *tff.f*
c-----------------------------------------------------------
c
      implicit real*8 (a-h,o-z)
c
      character*10 name,parameter
      dimension r(50)
c
      nh = 9
      np = 8
      open(19,file='tff.par')
      open(20,file='temp.dat')
c
      do 4 i=1,nh
      read(19,2) name
 4    continue
      do 1 i=1,np
      read(19,2) name,parameter
      write(20,2) parameter
 2    format(2(a10))
 1    continue
      close(19)
      close(20)
c
      open(20,file='temp.dat')
      do 3 i=1,np
      read(20,*) r(i)
  3   continue
      close(20)
c
      ntbin    = r(1)
      nmin     = r(2)
      mindp    = r(3)
      snr1min  = r(4)
      nmatch   = r(5)
      dph      = r(6)
      asig     = r(7)
      jfit     = r(8)
c
       return
       end
c
c
       subroutine tmax(imax,w,mord,epoch,am,ph,ext,tm)
c
c---------------------------------------------------------
c      This routine computes the moments from the Fourier 
c      decomposition.
c
c      - maximum brightness (imax=1)
c      - minimum brightness (imax=2)
c---------------------------------------------------------
c
       implicit real*8 (a-h,o-z)
c
       dimension am(20),ph(20),w(20)
c
       eps=1.0d-5
       peri=1.0d0/w(1)
       n=50
       tma=epoch
c
c...   Raw search
c   
       vmax=-1.0d30
       if(imax.eq.1) vmax=1.0d30
       dt=peri/dfloat(n)
c
       do 204 i=1,n
       time=epoch+dt*dfloat(i)
       call fsum(time,epoch,mord,w,am,ph,sum)
       if(imax.eq.2) go to 500
       if(sum.gt.vmax) go to 204
       go to 501
 500   continue
       if(sum.lt.vmax) go to 204
 501   continue
       vmax=sum
       tma=time
 204   continue
c
c...   Fine search
c
       rm=5.0d0
 100   continue
       dt=dt/2.0d0
       if(dt.lt.eps*peri) go to 102
       t1=tma-rm*dt
       n=1+idint(2.0d0*rm)
       do 101 i=1,n
       time=t1+dt*dfloat(i-1)
       call fsum(time,epoch,mord,w,am,ph,sum)
       if(imax.eq.2) go to 600
       if(sum.gt.vmax) go to 101
       go to 601
 600   continue
       if(sum.lt.vmax) go to 101
 601   continue
       vmax=sum
       tma=time
c
 101   continue
c
       go to 100   
c
 102   continue
       tm=tma
       ext=vmax
c
       return
       end
c
c
       subroutine fsum(time,epoch,mord,w,am,ph,sum)
c
c----------------------------------------------------------
c      This routine computes the value of a Fourier-sum
c----------------------------------------------------------
c
       implicit real*8 (a-h,o-z)
       dimension w(20),am(20),ph(20)
c
       PI2=8.0d0*datan(1.0d0)
       sum=0.0d0
       fac=pi2*(time-epoch)
       do 1 j=1,mord
       sum=sum+am(j)*dsin(fac*w(j)+ph(j))
 1     continue
c
       return
       end
c
c
      subroutine fourin(maxdi1,nmin,snr1min,n,sname,per,a0,amp,pha,sig)
c
c---------------------------------------------------------------------
c     This routine reads in Fourier decompositions from a file in 
c     a standard old (JK'96) format
c---------------------------------------------------------------------
c
      implicit real*8 (a-h,o-z)
c
      character*22 sname(843)
c
      dimension per(843),sig(843),a0(843)
      dimension amp(20,843),pha(20,843)
c
c     Read in Fourier decompositions 
      
      open(1,file='template.dat')
      n=0
 128  continue
      n=n+1
      if(n.gt.maxdi1) go to 124      
      read(1,129,end=124) sname(n)
 129  format(a22)
      read(1,*) per(n),epoch,a0(n),nn,sig(n)
      read(1,*) amp(1,n),pha(1,n),amp(2,n),pha(2,n),amp(3,n),pha(3,n),
     &amp(4,n),pha(4,n),amp(5,n),pha(5,n)
      read(1,*) amp(6,n),pha(6,n),amp(7,n),pha(7,n),amp(8,n),pha(8,n),
     &amp(9,n),pha(9,n),amp(10,n),pha(10,n)
      read(1,*) amp(11,n),pha(11,n),amp(12,n),pha(12,n),
     &amp(13,n),pha(13,n),amp(14,n),pha(14,n),amp(15,n),pha(15,n)
c
c..........................................................
c     Check number of data points, signal-to-noise ratio 
c
      igo=1
      if(nn.gt.nmin) go to 2
      n=n-1
      go to 128
 2    continue
      snr1=amp(1,n)/(sig(n)/dsqrt(dfloat(nn)))      
      if(snr1.gt.snr1min) go to 1
      n=n-1
      go to 128      
 1    continue
c..........................................................
c     
      go to 128
 124  continue
      close(1)
      n=n-1
c
      return
      end
c
c
      subroutine read1(fname,n,t,x)
c
c--------------------------------------------------------------------
c     This routine reads in data items from SIMPLE time series file.
c     Output time series is ordered by increasing time values. 
c--------------------------------------------------------------------
c
      implicit real*8 (a-h,o-z)
c
      character*70 fname 
c
      dimension t(3456),x(3456)
      dimension u(3456),v(3456)
c
      open(1,file=fname)
c
c...  Read in data from the SIMPLE time series file
c
      n=0
 1    continue
      n=n+1
      read(1,*,end=7) u(n),v(n)  
      go to 1
 7    continue
      n=n-1
      close(1)
c
c...  Order time series in  t(i+1) > t(i)
c
      do 10 i=1,n
      umin=1.0d30
      k=1
      do 11 j=1,n
      uu=u(j)
      if(uu.gt.umin) go to 11
      k=j
      umin=uu
 11   continue
      t(i)=u(k)
      x(i)=v(k)
      u(k)=1.0d30
 10   continue 
c
      return
      end
c
c
      subroutine filenamein3(sn,path,pert,nv)
c
c---------------------------------------------------------------------------
c     This routine reads in the available files and corresponding star  
c     names and periods
c---------------------------------------------------------------------------
c
c  Input:    --- target.lis 
c  ~~~~~~           
c
c  Output:   --- path(i) : File name with full path, containing the light
c  ~~~~~~~                 curve of the i-th variable 
c            --- sn(i)   : Name of the i-th variable
c            --- pert(i) : Period of the i-th star 
c            --- nv      : Total number of files in  'target.lis'  
c
c---------------------------------------------------------------------------
c
      implicit real*8 (a-h,o-z)
c
      character*22 sn(843)
      character*70 path(843)
      character*22 sname
      character*70 fname 
c
      dimension pert(843)
c
c---------------------------------------------------------------------------
c 
      open(13,file='target.lis')
c
      n = 0 
 8    continue 
      read(13,7,end=3) sname
      read(13,*)       period
      read(13,71)      fname
 7    format(a22)
 71   format(a70)
      n=n+1
      sn(n)   = sname
      pert(n) = period
      path(n) = fname
      go to 8
 3    continue
      close(13)
      nv=n
c
      return
      end
c
c
      subroutine tlc(inph,ntbin,mord,ntemp,per,amp,pha,vt)
c
c======================================================================
c     This routine computes template time series from the template 
c     Fourier decompositions
c======================================================================
c
c     Input parameters:  inph  = 1 --- start phased template time  
c     ~~~~~~~~~~~~~~~~~                series at the maximum brightness
c                                2 --- start phased template time  
c                                      series at the minimum brightness
c                        
c                        ntbin = Number of bins used in the computation 
c			         of the template time series
c
c                        mord  = Order of the Fourier sum used in the 
c                                generation of the synthetic template 
c                                time series
c
c                        ntemp = Number of template time series
c
c                        per(k)= Period [day] of the k-th template
c                        
c                        amp(i,k) = Fourier amplitude A_i of the i-th 
c                                   component (with i/per(k) frequency) 
c                                   of the k-th template
c
c                        pha(i,k) = Fourier phase phi_i of the i-th 
c                                   component (with i/per(k) frequency) 
c                                   of the k-th template
c
c     Output parameters: vt(j,k)  = Synthetic time series for one  
c     ~~~~~~~~~~~~~~~~~~            period of the k-th template. 
c                                   j=1,2,...,ntbin; j=1 corresponds 
c                                   to the MAXIMUM brightness, if 
c                                   inph=1, whereas it corresponds to 
c                                   the MINIMUM brightness, if inph=2
c
c======================================================================
c
      implicit real*8 (a-h,o-z)
c
      dimension per(843)
      dimension amp(20,843),pha(20,843)	
      dimension am(20),ph(20),w(20)
      dimension vt(843,843)
c
      epoch = 0.0d0
      fac   =-1.0d0
c
      do 1 k=1,ntemp
c
      peri = per(k)
      dt   = peri/dfloat(ntbin)
      freq = 1.0d0/peri
c
      do 3 j=1,mord
      am(j)= amp(j,k)
      ph(j)= pha(j,k)
      w(j) = j*freq
 3    continue
c
      call tmax(inph,w,mord,epoch,am,ph,ex1,tm) 
c
      do 2 i=1,ntbin
      time=tm+dt*dfloat(i-1)
      call fsum(time,epoch,mord,w,am,ph,sum)
      vt(i,k)=fac*sum
 2    continue
c
 1    continue
c
      return
      end
c
c
      subroutine foldex(n,u,v,p0,ep0,plotph,npbin,nfold,x,y)
c
c---------------------------------------------------------------------
c     This routine folds {u(i),v(i); i=1,2,...,n} into 
c                        {x(j),y(j); i=1,2,...,npbin} 
c
c     Folding period        = p0
c     Epoch of the folding  = ep0  [ i.e., ph(i)=FRAC((u(i)-ep0)/p0) ]
c                             It is assumed that ep0<u(i) for ALL 'i'
c
c     The folded array is extended to npbin*plotph=nfold data points
c---------------------------------------------------------------------    
c
      implicit real*8 (a-h,o-z)
c
      dimension x(3456),y(3456)
      dimension u(3456),v(3456),b(3456)
c
      umin=ep0
c
c...  Check if umin<u(i) for all 'i'
c
      is=0
      do 7 i=1,n
      if(u(i).lt.umin) is=1
 7    continue
      if(is.eq.1) write(*,*) ' u(i) < umin  !!!' 
      if(is.eq.1) stop
c
c...  Initialize bin arrays
c
      do 3 i=1,npbin
      b(i)=0.0
      y(i)=0.0
 3    continue
c
c...  Folding
c
      dx=1.0/dfloat(npbin)
      f0=1.0/p0
      do 2 i=1,n
      ph = (u(i)-umin)*f0
      ph = ph-idint(ph)
      j  = 1+idint(ph*npbin)
      b(j) = b(j)+1.0
      y(j) = y(j)+v(i)
c
 2    continue
      do 4 j=1,npbin
      if(b(j).gt.0.5) y(j)=y(j)/b(j)
      x(j)=(j-1)*dx
 4    continue
c
c...  Extend array to npbin*plotph
c
      nfold=idint(npbin*plotph)
      n1=npbin+1
      do 5 j=n1,nfold
      x(j)=(j-1)*dx
      k=j-npbin
      y(j)=y(k)
      b(j)=b(k)
 5    continue
c
c...  Omit empty bins
c
      k=0
      do 6 i=1,nfold
      if(b(i).lt.0.5) go to 6
      k=k+1
      x(k)=x(i)
      y(k)=y(i)
 6    continue
      nfold=k
c
      return
      end
c
c
      subroutine outlier(sigcut,n,u,v,w,nn,nout)
c
c--------------------------------------------------------------------
c     This routine checks if  |v(i)-w(i)| < sigcut 
c
c     Input:  sigcut -- only those data points are kept that 
c     ~~~~~~            satisfy the |v(i)-w(i)| < sigcut condition
c             n      -- number of data points
c             u(i)   -- independent variable of the function  v(u)
c             v(i)   -- dependent   variable of the function  v(u)
c             w(i)   -- comparison function, counterpart of v(i)
c
c     Output: nn     -- number of data pints left after outlier 
c     ~~~~~~~           selection
c             nout   -- number of outliers:    nout = n - nn
c             u(i)   -- u(i) after outlier selection
c             v(i)   -- v(i) after outlier selection
c
c     Warning: Output arrays {u(i),v(i)} is DIFFERENT from the input 
c     ~~~~~~~~ array if  nn.NE.n !!
c--------------------------------------------------------------------
c
      implicit real*8 (a-h,o-z)
c
      dimension u(3456),v(3456),w(3456)
c
      nout=0
      nn=0
      do 1 i=1,n
      d=dabs(v(i)-w(i))
      if(d.gt.sigcut) go to 1
      nn=nn+1
      u(nn)=u(i)
      v(nn)=v(i)
 1    continue
      nout=n-nn
c
      return
      end
c
C
       SUBROUTINE MATINV(M,G)
C      MATRIX INVERSION
       IMPLICIT REAL*8 (A-H,O-Z)
C      INPUT: G(M3,M3), OUTPUT: G(M3,M3)
       DIMENSION G(40,41)
       M3=M
       DO 11 I=1,M3
       W=G(I,I)
       Q=1.0D+00/W
       DO 12 K=1,M3
       G(I,K)=Q*G(I,K)
       IF(K.NE.I) GO TO 12
       G(I,K)=Q
  12   CONTINUE
       DO 13 J=1,M3
       IF(J.EQ.I) GO TO 13
       TT=G(J,I)
       Q1=-TT/W
       DO 14 K=1,M3
       G(J,K)=G(J,K)-TT*G(I,K)
       IF(K.NE.I) GO TO 14
       G(J,K)=Q1
  14   CONTINUE
  13   CONTINUE
  11   CONTINUE
       RETURN
       END
C
       SUBROUTINE NORM(N,M,T,X,F,G)
C      CALCULATION OF THE ELEMENTS OF THE NORMAL MATRIX AND R.H.S.
       IMPLICIT REAL*8 (A-H,O-Z)
       DIMENSION G(40,41),T(3456),X(3456),F(40)
       P7=8.0D0*DATAN(1.0D0)
       M3=2*M+1
       M4=M3+1
       L=0
       G(1,1)=FLOAT(N)
       DO 1 J=2,M3,2
       S=0.0D+00
       C=0.0D+00
       L=L+1
       J1=J+1
       FL=F(L)
       DO 2 I=1,N
       PH=T(I)*FL
       PH=P7*PH
       S=S+DSIN(PH)
  2    C=C+DCOS(PH)
       G(1,J)=C
  1    G(1,J1)=S
       L1=0
       DO 3 J=2,M3,2
       L1=L1+1
       J1=J+1
       L2=L1-1
       FL1=F(L1)
       DO 4 K=J,M3,2
       K1=K+1
       L2=L2+1
       FL2=F(L2)
       S1=0.0D+00
       S2=0.0D+00
       C1=0.0D+00
       C2=0.0D+00
       DO 5 I=1,N
       PH1=T(I)*FL1
       PH1=P7*PH1
       PH2=T(I)*FL2
       PH2=P7*PH2
       SPH1=DSIN(PH1)
       CPH1=DCOS(PH1)
       SPH2=DSIN(PH2)
       CPH2=DCOS(PH2)
       C1=C1+CPH1*CPH2
       S1=S1+CPH2*SPH1
       C2=C2+SPH2*CPH1
  5    S2=S2+SPH1*SPH2
       G(J,K)=C1
       G(J,K1)=C2
       G(J1,K)=S1
  4    G(J1,K1)=S2
  3    CONTINUE
       S=0.0D+00
       DO 6 I=1,N
  6    S=S+X(I)
       G(1,M4)=S
       L=0
       DO 7 J=2,M3,2
       S=0.0D+00
       C=0.0D+00
       L=L+1
       J1=J+1
       FL=F(L)
       DO 8 I=1,N
       PH=T(I)*FL
       PH=P7*PH
       S=S+X(I)*DSIN(PH)
  8    C=C+X(I)*DCOS(PH)
       G(J,M4)=C
  7    G(J1,M4)=S
       DO 9 J=1,M3
       DO 10 K=J,M3
  10   G(K,J)=G(J,K)
   9   CONTINUE
       RETURN
       END
C
C     
      subroutine fourdeco(asig,t0,p0,m,n,t,x,
     &ts,xs,nn,sig,a00,aa,pp)
C
C=========================================================================
C     THIS CODE FITS A FOURIER SUM TO A TIME-SERIES THROUGH AN ITERATIVE 
C     OMISSION OF OUTLYING DATA POINTS
C=========================================================================
C
C     WARNING:  Upon return, the original time series 
C     ~~~~~~~~  {t(i),x(i)} i=1,2,...,n  will be SUBSTITUTED by the 
C               time series which have been obtained by the omission 
C               of the outliers -- this new time series contains 'n' 
C               data points, which is lower than the original 'n'
C
C     INPUT:  ifit --  key to determine the type of fit:
C     ~~~~~~
C             ifit = 1 >>> a Fourier sum with f0,2*f0, ..., m*f0 
C                          frequencies are fitted (f0=1/p0)
C
C             ifit = 2 >>> a Fourier sum with f0,f1 (m=2), 
C                          f0,f1,2*f0,2*f1,f0+f1,f1-f0 (m=6), etc. 
C                          (see frequency setting below) are fitted
C
C             ifit = 3 >>> a Fourier sum with f0,f0-df,f0+df,2*f0, 
C                          3*f0,4*f0,5*f0,6*f0 (m=8), or some other 
C                          combinations (see frequency setting below) 
C                          are fitted
C 
C             asig  -- outliers are iteratively omitted 
c                      at the   asig*sigma   level 
c             t0    -- epoch
C             p0    -- first  period
C             p1    -- second period (used only if ifit=2)
C             dfm   -- modulation frequency (used only if ifit=3) 
C             m     -- order of the fit ( < 21 )
C             n     -- number of data
C             t(i)  -- time
C             x(i)  -- time series
C             nn    -- number of data points for the synthetic signal
C
C     OUTPUT: xs(i) -- synthetic data, i=1,2,...,nn; data are generated 
C     ~~~~~~~          in equidistant time steps from t(1) to t(1)+deltat, 
C                      where: 
C
C                      deltat = 1/p0          , if ifit=1
C                             = 1/(1/p1-1/p0) , if ifit=2
C                             = 1/dfm         , if ifit=3
C
C             sig   -- sigma of the resulting fit
C             a00   -- zero frequency component
C             aa(j) -- amplitude of the j-th component
C             pp(j) -- phase     of the j-th component
C
C     METHOD OF OUTLIER OMISSION: At each iteration ALL data points 
C     ~~~~~~~~~~~~~~~~~~~~~~~~~~~ deviating more than  asig*sigma  will 
C                                 be omitted, and the procedure is repeated 
C                                 until no outlier is found, or the number 
C                                 of data points becomes lower than 
C                                 minn=4*m, where 'm' is the order of 
C                                 the Fourier fit.
C..............................................................
C
C     FOURIER DECOMPOSITION HAS THE FOLLOWING FORM:
C
C     A0 + A1*SIN(2*PI*F1*(T-T0)+FI1) + ...
C
C..............................................................
C
C==============================================================
C
      IMPLICIT REAL*8 (A-H,O-Z)
C
      DIMENSION X(3456),T(3456),G(40,41),W(40),F(40)
      DIMENSION U(3456),V(3456),TS(3456),XS(3456)
      DIMENSION A(40),B(40),AF(40),AA(20),PP(20)
      DIMENSION PDA(40)
      DIMENSION IND(3456)
C
      MINN   = 4*M
      P7     = 8.0D0*DATAN(1.0D0)
      NNN    = NN
      ifit   = 1
c
c...  Dummy parameters, just to keep the original routine 'fdeco' 
c     without introducing unnecessary changes
c
      p1     = 0.11111
      dfm    = 0.01111
c
c...  Set frequencies if  ifit=1
c
      if(ifit.ne.1) go to 6543
      W(1)=1.0D0/P0
      DO 887 I=1,M
      W(I)=W(1)*DFLOAT(I)
 887  CONTINUE
      DELTAT=P0
      GO TO 5011
c
c...  Set frequencies if  ifit=2
c
 6543 continue
      if(ifit.ne.2) go to 6544
      w(1)=1.0d0/p0
      w(2)=1.0d0/p1
      w(3)=2.0d0*w(1)
      w(4)=2.0d0*w(2)
      w(5)=w(2)-w(1)
      w(6)=w(2)+w(1)
      w(7)=3.0d0*w(1)
      w(8)=3.0d0*w(2)
      w(9)=2.0d0*w(1)+w(2)
      w(10)=2.0d0*w(2)+w(1)
      w(11)=2.0d0*w(1)-w(2)
      w(12)=2.0d0*w(2)-w(1)
      w(13)=4.0d0*w(1)
      w(14)=4.0d0*w(2)
      w(15)=3.0d0*w(1)-w(2)
      w(16)=3.0d0*w(1)+w(2)
      w(17)=3.0d0*w(2)-w(1)
      w(18)=3.0d0*w(2)+w(1)
      w(19)=2.0d0*(w(2)-w(1))
      w(20)=2.0d0*(w(2)+w(1))
      deltat=1.0d0/(w(2)-w(1))
      go to 5011
c
c...  Set frequencies if   ifit=3
c
 6544 continue
      if(ifit.ne.3) write(*,*) ' Wrong value for  *ifit* !'
      if(ifit.ne.3) stop 
      f0   = 1.0d0/p0
      w(1) = f0-dfm
      w(2) = f0
      w(3) = f0+dfm
      w(4) = 2*f0
      w(5) = 3*f0
      w(6) = 4*f0
      w(7) = 5*f0
      w(8) = 6*f0 
c
 5011 continue
c
       DO 30 I=1,N
       T(I)=T(I)-T0
       U(I)=T(I)
       V(I)=X(I)
 30    CONTINUE
C
C*************************************************
C      START CYCLE FOR OMITTING DISCREPANT DATA
C*************************************************
C
 100   CONTINUE
C
       DO 102 I=1,N
       T(I)=U(I)
       X(I)=V(I)
 102   CONTINUE
C
c==============================================
c      Fit a Fourier series to
c      {t(i),x(i); i=1,2,...,n}
c
       DO 300 J=1,M
       F(J)=W(J)
 300   CONTINUE
C
       M2=2*M
       M3=M2+1
       M4=M3+1
       CALL NORM(N,M,T,X,F,G)
       CALL MATINV(M3,G)
       A0=0.0D+00
       DO 31 J=1,M3
  31   A0=A0+G(1,J)*G(J,M4)
       DO 32 J=2,M3
       S=0.0D+00
       DO 33 K=1,M3
  33   S=S+G(J,K)*G(K,M4)
       A(J)=S
       AF(J)=S
  32   CONTINUE
C
       K=0
       DO 37 J=2,M3,2
       K=K+1
       J1=J+1
       AMP=DSQRT(A(J)*A(J)+A(J1)*A(J1))
       PH=A(J1)/AMP
       PH=DACOS(PH)
       IF(A(J).LT.0.0D+00) PH=P7-PH
       B(K)=PH
       A(K)=AMP
       AA(K)=AMP
       PP(K)=PH
       PDA(J)=A(J)
       PDA(J1)=A(J1)       
  37   CONTINUE
       A00=A0
c
c==============================================
c      Compute sigma of the fit    
c     
       DISP=0.0D0
C
       DO 2000 I=1,N
       S=A0
       TIME=T(I)
       DO 2001 K=1,M
       PHASE=F(K)*TIME
       S=S+A(K)*DSIN(P7*PHASE+B(K))
 2001  CONTINUE
       XS(I)=S
       DEVI=X(I)-S
       DISP=DISP+DEVI*DEVI
 2000  CONTINUE
       DISP=DSQRT(DISP/DFLOAT(N-2*M-1))
c
c==============================================
c      Select outliers   
c
       dev=asig*disp
       nout=0
       do 120 i=1,n
       ind(i)=1
       d=dabs(x(i)-xs(i))
       if(d.lt.dev) go to 120
       ind(i)=0
       nout=nout+1
 120   continue
       if(nout.eq.0) go to 122
       if(n-nout.lt.minn) go to 122
c
       nn=0
       do 121 i=1,n
       if(ind(i).eq.0) go to 121
       nn=nn+1
       u(nn)=t(i)
       v(nn)=x(i)
 121   continue
       n=nn
       go to 100
 122   continue
c
       SIG = DISP
C
C      CALCULATE SYNTHETIC TIME SERIES
C
C      Equidistant time step is used in the interval  'deltat'
C
       NN = NNN       
       DT = DELTAT/DFLOAT(NN-1)
C
       DO 2200 I=1,NN
       S=A0
       TIME=T(1)+DT*DFLOAT(I-1)
       TS(I)=TIME
       DO 2201 K=1,M
       PHASE=F(K)*TIME
       S=S+A(K)*DSIN(P7*PHASE+B(K))
 2201  CONTINUE
       XS(I)=S
 2200  CONTINUE
C
C
       RETURN
       END
C
c
      subroutine minmax(n,x,xmin,xmax)
c
c     This routine computes (min,max) of {x(i); i=1,...,n}
c
      implicit real*8 (a-h,o-z)
c
      dimension x(3456)
c
      xmin = 1.0d30
      xmax =-1.0d30
      do 1 i=1,n
      xmin = dmin1(xmin,x(i))
      xmax = dmax1(xmax,x(i))
 1    continue
c
      return
      end
c
c
      subroutine dfdeco(asig,t00,p0,n,tin,xin,
     &nsynt,ts,xs,sigma,a00,aa,pp,mopt)
c
c------------------------------------------------------------------------
c     This routine computes DIRECT FOURIER DECOMPOSITION with outlier 
c     selection and optimum Fourier order
c
c     Optimum Fourier Order: Defined as the maximum order at which:  
c     ~~~~~~~~~~~~~~~~~~~~~~ (a) the Fourier amplitudes are still 
c                                monotonically decreasing (up to the 
c                                factor 'facam')
c                            (b) the unbiased estimate of fitting 
c                                accuracy (the r.m.s of the residuals 
c                                between the fit and the data) is 
c                                minimum.
c                            (c) The total amplitude of the fitted 
c                                curve is not larger than 'factam*A_tot', 
c                                where A_tot is the total amplitude of 
c                                the target signal.
c------------------------------------------------------------------------
c
c     Input:    asig  -- outlying data points are omitted at the 
c     ~~~~~~             asig*sigma level (sigma=standard deviation 
c                        of the residuals between the data and the 
c                        fit)
c               t00   -- epoch of the fit (i.e., the fit is computed 
c                        on the time base of {tin(i)-t00}
c               p0    -- period 
c               n     -- number of data points of the input time series 
c                        {tin(i),xin(i)}
c               tin(i)-- time values
c               xin(i)-- time series values
c
c     Output:   nsynt -- number of data points in the synthetic time   
c     ~~~~~~~            series {ts(i),xs(i)}
c               ts(i) -- time values of the synthetic time series
c               xs(i) -- time series values of the synthetic time series
c               sigma -- standard deviation between the synthetic and 
c                        input data
c               a00   -- zero frequency component of the Fourier 
c                        decomposition
c               aa(j) -- amplitude of the j-th Fourier component
c               pp(j) -- phase of the j-th Fourier component
c               mopt  -- order of the Fourier fit
c
c     Remark:   Please relax, the input time series will not change in 
c     ~~~~~~~   spite of the sigma clipping, because the time series 
c               is copied into a temporal array.
c------------------------------------------------------------------------
c
      implicit real*8 (a-h,o-z)
c
      dimension tin(3456),xin(3456)
      dimension x(3456),y(3456),u(3456),v(3456)
      dimension aa(20),pp(20),ts(3456),xs(3456)    
c
c------------------------------------------------------------------------
c
c     SET BASIC PARAMETERS
c
      maxord = 15
      facam  = 1.2
      factam = 1.2
      nsynt  = 200
c
c...  Modify  'maxord'  if there are too few data points
c
      maxo   = (n-1)/2
      if(maxo.lt.maxord) maxord=maxo
c
c     maxord = Maximum Fourier order to be tested
c     facam  = If  A_(i-1) < facam*A_i  at a given order, then no 
c              higher order tests are made.
c     factam = If the synthetic curve has a total amplitude greater 
c              than  factam*A_tot, then the result of the corresponding 
c              order is not considered (A_tot is the total amplitude 
c              of the target time series).
c     nsynt  = Synthetic time series of length *po* is generated with 
c              *nsynt*  data points. 
c
c*********************************************
c     Compute DIRECT FOURIER DECOMPOSITION   *
c*********************************************
c
c...  Copy original time series to a temporal one, in order 
c     to leave the original time series unchanged
c
      do 5 i=1,n
      u(i)=tin(i)
      v(i)=xin(i)
 5    continue
c
c...  Estimate optimum order of the Fourier fit
c
      t0   = t00
      nn   = nsynt 
      dsig = 55555.0      
c
c...  Compute (min,max) of the input folded time series
c 
      nb     = n
      epoch  = 0.0
      plotph = 1.0
      npb    = 20
c
      call foldex(nb,u,v,p0,epoch,plotph,npb,in,x,y) 
      call minmax(in,y,ymin,ymax)
      atot = ymax-ymin
c
c...  Picking order that yields the smallest sigma
c     and monotonic decreasing A_i
c
c     No outlier selection is applied
c
      sig8=1.0d30
c
      do 1 m=1,maxord
c
      igo=1
c
      call fourdeco(dsig,t0,p0,m,n,u,v,
     &ts,xs,nn,sig7,a00,aa,pp)
      call minmax(nn,xs,xmin,xmax)
      xtot=xmax-xmin
c
      if(m.lt.2) go to 3
      do 2 j=2,m
      j1=j-1
      if(aa(j).gt.aa(j1)*facam) igo=0
 2    continue
 3    continue
c
      if(xtot.gt.atot*factam) go to 1
      if(igo.eq.0)            go to 1
      if(sig7.gt.sig8)        go to 1
      sig8=sig7
      mopt=m
      btot=xtot/atot
c
 1    continue
c
c...  Initializing Fourier arrays  
c
      do 110 j=1,maxord
      aa(j)=0.0d0
      pp(j)=0.0d0
 110  continue
c     
      call fourdeco(asig,t0,p0,mopt,n,u,v,
     &ts,xs,nn,sigma,a00,aa,pp)
c
      return
      end
c
c
      subroutine phrange(ph,phref)
c
c---------------------------------------------------------------------
c     This routine finds the (mod 2*pi) closest value of ph to phref
c---------------------------------------------------------------------
c
      implicit real*8 (a-h,o-z)
c
      pi2=6.28318593d0
      n=60
      n1=30
      dif=1.0d30
      dev=dabs(ph-phref)
      p=ph-pi2*dfloat(n1)
      pbest=p
c
      do 1 i=1,n
      p=p+pi2
      d=dabs(p-phref)
      if(d.gt.dif) go to 1
      dif=d
      pbest=p
 1    continue
c
      if(dif.lt.dev) ph=pbest                   
c   
      return
      end 
c
c
      subroutine bestfour(jfit,c,vt,ntbin,kmin,
     &period,m,amp,pha,a0b,am,ph,p31)
c
c---------------------------------------------------------------------
c     This routine computes the Fourier decomposition of the 
c     best fitting template time series
c---------------------------------------------------------------------
c     Input:   jfit, c(j), vt(i,k), ntbin, kmin, period, m
c     ~~~~~~   {amp(j,k),pha(j,k); j=1,2,...,mord1; k=1,2,...,maxdi1}
c
c     Output:  a0b, {am(j),ph(j); j=1,2,...,mord1}, p31
c     ~~~~~~~
c---------------------------------------------------------------------      
c
      implicit real*8 (a-h,o-z)
c	
      dimension am(20),ph(20),c(11)
      dimension amp(20,843),pha(20,843)	
      dimension x(3456),y(3456)
      dimension vt(843,843)
      dimension ts(3456),xs(3456)
c
      asig9=9.9d29
      n=ntbin
      dt=period/dfloat(n)  
      nd=jfit
      nd1=nd+1
      if(nd.eq.0) nd1=2
c
c...  Compute time series of the best TRANSFORMED template
c
      do 2 i=1,n
      x(i)=dt*dfloat(i)
      bb=vt(i,kmin)
      s=c(nd1)
      do 21 j=1,nd
      j1=nd1-j
      s=s*bb+c(j1)
 21   continue
      y(i)=s
      if(nd.eq.0) y(i)=c(1)+c(2)*bb
 2    continue
      t00=x(1)
      nn=1
c
c...  Compute DFF of the best TRANSFORMED template
c
      call fourdeco(asig9,t00,period,m,n,x,y,
     &ts,xs,nn,sigma,a0b,am,ph)
c
c...  phi_31 
c
 3    continue
      phref  = 5.1
      p31 = ph(3)-3.0*ph(1)
      call phrange(p31,phref)
c
      return
      end
c
c
      subroutine polynfit(itp,isp,n,t,x,nd,nn,ti,xi,c,sigma)
c
c-------------------------------------------------------------------------
c     This is a polynomial fitting routine
c-------------------------------------------------------------------------
c
c     Input:  itp = 0 -- Use array {t(i)} as the independent variable
c     ~~~~~~             in the polynomial fit.
c                   1 -- Transform {t(i)} to the interval [-1,1] before 
c                        using it as the independent variable in the 
c                        polynomial fit.
c
c             isp = 0 -- Compute equidistant interpolated time series
c                   1 -- Use the same values of {t(i)} for interpolation
c 
c               n = Number of the data points of the input function
c
c          {t(i)} = Values of the independent variable of the function 
c                   to be fitted.
c
c          {x(i)} = Values of the dependent variable of the function 
c                   to be fitted.                    
c
c              nd = Degree of the polynomial to be fitted.
c
c              nn = Number of the interpolated data points of the 
c                   output function {ti(i),xi(i)}. If 'isp=1', then 
c                   nn=n.
c
c    Output:  {ti(i)} = Values of the independent variable of the 
c    ~~~~~~~            interpolant. ti(1)=t(1), ti(nn)=t(n) and 
c                       {ti(i)=t(i)} if 'is=1', and ti(k)=t(1)+dt*(i-1), 
c                       with dt=(t(n)-t(1))/(n-1), if 'is=0'.
c
c             {xi(i)} = Values of the dependent variable of the 
c                       interpolant.
c             
c             {c(i)}  = Regression coefficients, 1 < i < nd+1
c
c               sigma = Unbiased estimate of the standard deviation 
c                       of the fit
c=========================================================================
c
      implicit real*8 (a-h,o-z)
c
      dimension t(3456),g(40,41)
      dimension x(3456),c(11),td(11,3456)
      dimension ti(3456),xi(3456),u(3456)
c
c-------------------------------------------------------------------------
c
      nmax=3456
      ndmax=11    
      if(n.gt.nmax) write(*,*) 'Too many data points!'
      if(n.gt.nmax) return
      if(nd+1.gt.ndmax) write(*,*) 'Too high degree polynomial!'
      if(nd+1.gt.ndmax) nd=ndmax
c
c
c...  Change  nd  if the number of data is too low
c
      if(nd.ge.n-1) nd=n-1
c
c...  Set other parameters
c
      np = nd+1
      np1= np+1
      nd1= np
      it = itp
      is = isp
c
      if(is.ne.1) go to 1
      if(n.ne.nn) write(*,100) 
 100  format(' In the *polynfit*: nn must be equal to n if is=1!')
      nn=n 
 1    continue 
c
c-------------------------------------------------------------------------
c
c...  Transform  {t(i)}
c
      call minmax(n,t,tmin,tmax)
c
      r=2.0d0/(tmax-tmin)
      tmid=(tmin+tmax)/2.0d0
      do 511 i=1,n
      u(i)=t(i)
      if(it.gt.0) u(i)=r*(t(i)-tmid)
 511  continue
c
c...  Compute the various power of   t(i), i=1,2,...,n
c
      do 22 i=1,n
      td(1,i)=1.0d0 
 22   continue
      if(np.lt.2) go to 23
c     
      do 500 i=1,n
      do 501 j=2,np
      k=j-1
      td(j,i)=u(i)*td(k,i)
 501  continue
 500  continue      
 23   continue
c
c************************
c     Compute the fit   *
c************************
c
      do 1001 j=1,np
      c(j)=0.0d0
 1001 continue
c
c...  Computation of the normal matrix
c
      do 11 i=1,np
      do 12 j=i,np
      s=0.0d0
      do 13 k=1,n
      s=s+td(i,k)*td(j,k)
 13   continue
      g(i,j)=s
 12   continue
 11   continue
      do 53 i=1,np
      do 54 j=i,np
      g(j,i)=g(i,j)
 54   continue
 53   continue
c
      do 55 i=1,np
      s=0.0d0
      do 14 k=1,n
      s=s+td(i,k)*x(k)
 14   continue
      g(i,np1)=s
 55   continue
c
c...  Compute solution
c
      call matinv(np,g)
      do 16 i=1,np
      s=0.0d0
      do 17 j=1,np
      s=s+g(i,j)*g(j,np1)
 17   continue
      c(i)=s
 16   continue
c
c...  Compute interpolated time series and 'sigma'
c
      dt=(tmax-tmin)/dfloat(nn-1)
c
      do 520 i=1,nn
      ti(i)=tmin+dt*dfloat(i-1)
      if(is.eq.1) ti(i)=t(i)
      time=ti(i)
      if(it.gt.0) time=r*(ti(i)-tmid)
      s=c(nd1)
      do 521 j=1,nd
      j1=nd1-j
      s=s*time+c(j1)
 521  continue
      xi(i)=s
 520  continue
c
      sigma=0.0
      do 20 i=1,n
      time=u(i)
      s=c(nd1)
      do 21 j=1,nd
      j1=nd1-j
      s=s*time+c(j1)
 21   continue
      d=x(i)-s
      sigma=sigma+d*d
 20   continue
      sigma=dsqrt(sigma/dfloat(n-nd1))
c
      return
      end
c
c
      subroutine tfitpol(p0,n,x,y,xmin,ntbin,vt,itemp,dph,iq,
     &nd,vtf,c,sigma,epoch)
c
c**************************************************************************
c     This routine computes the best fit of a template time series to 
c     a target. 
c     >>>>>>>>> nd-th order polynomial fit 
c**************************************************************************
c
c     Input:   p0      :: period of the target time series    
c     ~~~~~~   n       :: number of data points for the target time series
c              x(i)    :: time values for the target time series
c              y(i)    :: time series values for the target time series
c              xmin    :: MIN{x(i)}
c              ntbin   :: number of sampling data points for each time 
c                         series in the template set
c              vt(i,k) :: template set (i=1,2,...,ntbin; k=1,2,...,ntemp)
c              itemp   :: template index; (k=itemp in the above array)
c              dph     :: Template time series are fitted with an accuracy 
c                         of  *dph*  in phase
c              iq      :: 0 -- for no interpolation
c                         1 -- for quadratic interpolation
c              nd      :: order of the polynomial fit
c
c     Output:  vtf(i)  :: best fitted template time series, 
c     ~~~~~~~             i=1,2,...,n; given on the time base of {x(i)}, 
c                         which is the same as for {y(i)}
c              {c(i)}  :: regression coefficients; 
c                         target time series are fitted by minimizing the 
c                         following expression: 
c
c                         SUM [ y(i) - SUM{c(j)*(vtf(i)**(j-1)} ]**2
c
c              sigma   :: standard deviation of the residuals between the 
c                         best fit and the target data points
c
c              epoch   :: epoch used in the best folding, i.e., the best 
c                         fitting template time series is obtained by 
c                         using a phasing of  ph   = (x(i)-epoch)/p0
c
c      Remark:  - If  nd=0, then only c(1) is computed and c(2) is 
c      ~~~~~~~    set equal to 1.0
c
c==========================================================================
c
      implicit real*8 (a-h,o-z)
c
      dimension x(3456),y(3456),z(3456)
      dimension vt(843,843),ti(3456),xi(3456),vtf(3456),v(3456)
      dimension c(11),cm(11)
c
      f0=1.0d0/p0
      e1=0.5*p0
      e2=e1+p0
      ne=50
      iter=0
      dph1=1.0d0/dfloat(ntbin)
      rdph2=0.5d0/(dph1*dph1)
      nd1=nd+1
      if(nd.eq.0) nd1=2
c
c...  Store temporal array
c
      do 10 i=1,ntbin
      z(i)=vt(i,itemp)
 10   continue     
c
 6    continue
c
      iter=iter+1
      de=(e2-e1)/dfloat(ne)
      sig=1.0d30
      kmin=1
c
      do 1 k=1,ne
c
      x1 = xmin-e1-(k-1)*de
c
c...  Compute bin indices of the target time series
c
      do 2 i=1,n
      ph   = (x(i)-x1)*f0
      ph   = ph-idint(ph)
      j    = 1+idint(ph*ntbin)
      v(i) = z(j)
      if(iq.eq.0) go to 2
c      
c...  Quadratic interpolation
c 
      if(j.eq.1)     j=2
      if(j.eq.ntbin) j=ntbin-1
      j1  = j-1
      j2  = j+1
      ph1 = ph-j1*dph1
      ph2 = ph1-dph1
      ph3 = ph2-dph1
      aa  = ph2*ph3
      bb  = ph1*ph3
      cc  = ph1*ph2
      v(i)= rdph2*(z(j1)*aa-2.0*z(j)*bb+z(j2)*cc)
c
 2    continue
c
c...  Compute polynomial fit
c
c     itp = 0 -- Use array {v(i)} as the independent variable
c                in the polynomial fit.
c           1 -- Transform {v(i)} to the interval [-1,1] before 
c                using it as the independent variable in the 
c                polynomial fit.
c
c     isp = 0 -- Compute equidistant interpolated time series
c           1 -- Use the same values of {v(i)} for interpolation
c
      if(nd.eq.0) go to 11 
      itp=0
      isp=1
      nn=n
      call polynfit(itp,isp,n,v,y,nd,nn,ti,xi,c,s)
      go to 12
c
 11   continue
c
c...  Compute LS fit if  nd=0
c
      cc=0.0
      do 13 i=1,n
      cc=cc+y(i)+v(i)
 13   continue
      cc=cc/dfloat(n)
      s=0.0
      do 14 i=1,n      
      xi(i)=cc-v(i)
      d=y(i)-xi(i)
      s=s+d*d
 14   continue
      s=dsqrt(s/dfloat(n-1))
      c(1)=cc
      c(2)=-1.0
c
 12   continue
c
      if(s.gt.sig) go to 1
c
c...  Store best fitting template time series
c
      sig=s
      kmin=k
      epoch=x1
      do 5 j=1,nd1
      cm(j)=c(j)
 5    continue
      do 4 i=1,n
      vtf(i)=xi(i)
 4    continue     
c
 1    continue
c
      df=de/p0
      if(df.lt.dph) go to 8
      e3 = e1+(kmin-1)*de
      e1 = e3-3.0*de
      e2 = e3+3.0*de
      go to 6     
c
 8    continue
c
      sigma=sig
      do 7 j=1,nd1
      c(j)=cm(j)
 7    continue
c
      return
      end
c
c
      subroutine bestfit(itest,period,tstar,na,ta,xa,
     &iq,jfit,asig,ntbin,dph,ntemp,sname,vt,
     &dev,era,num,nm,x1,y1,y2,devmin,epoch0,c,kmin)
c
c==========================================================================
c     This routine finds the best template from the template set
c
c     Input:   itest, period, tstar, na, ta, xa,
c     ~~~~~~   iq, jfit, asig, ntbin, dph, ntemp, sname, vt 
c
c     Output:  dev, era, num, nm, x1, y1, y2, devmin, epoch0, 
c     ~~~~~~~  c, kmin
c==========================================================================
c
      implicit real*8 (a-h,o-z)
c
      character*22 tstar,sname(843)
c
      dimension ta(3456),xa(3456),x(3456),y(3456)
      dimension x1(3456),y1(3456),y2(3456)
      dimension vt(843,843),c(11),cm(11)
      dimension dev(843),era(843),vtf(3456)
      dimension num(843)
c
c==========================================================================
c
      nd=jfit
      nd1=nd+1
      if(nd.eq.0) nd1=2
      ermin=1.0d30
      call minmax(na,ta,tmin,tmax)
c
      do 200 k=1,ntemp
c
      dev(k)=1.0d20
      era(k)=1.0d20
      num(k)=na
      if(itest.ne.3) go to 220
      if(tstar.eq.sname(k)) go to 200
 220  continue
c
      n=na
      do 201 i=1,n
      x(i)=ta(i)
      y(i)=xa(i)
 201  continue
c
 205  continue
c
      call tfitpol(period,n,x,y,tmin,ntbin,vt,k,dph,iq,
     &nd,vtf,c,sigma,epoch)
c      
      sigcut=asig*sigma
      call outlier(sigcut,n,x,y,vtf,nn,nout)
c
      n=nn     
      if(nout.gt.0) go to 205
c
      dev(k)=sigma
      num(k)=n
      error =sigma/dsqrt(dfloat(n))      
      era(k)=error
c            
      if(ermin.lt.error) go to 200
c
      ermin=error
      kmin=k
      devmin=sigma
      nm =n
      epoch0=epoch
      do 1 j=1,nd1
      cm(j)=c(j)
 1    continue
      do 206 i=1,nm
      x1(i)=x(i)
      y1(i)=y(i)
      y2(i)=vtf(i)
 206  continue
c      
 200  continue
c
      do 2 j=1,nd1
      c(j)=cm(j)
 2    continue
c
      return
      end
c
c
      subroutine topmatch(n,ntop,sfit,itop)
c
c-------------------------------------------------------------------
c     This code finds the first LOWEST *ntop* values from the array 
c     {sfit(i); i=1,2,...,n}
c
c     Array indices of these items are stored in
c     {itop(j); j=1,2,...,ntop}
c-------------------------------------------------------------------
c
      implicit real*8 (a-h,o-z)
c
      dimension sfit(843),s(843)
      dimension itop(843)
c
      k=1 
      do 3 i=1,n
      s(i)=sfit(i)
 3    continue
c
      do 1 j=1,ntop
      sig=1.0d30     
      do 2 i=1,n
      if(s(i).gt.sig) go to 2
      k=i
      sig=s(i)
 2    continue
      itop(j)=k
      s(k)=1.0d20
 1    continue
c
      return
      end
c
c

cc
cc    Compile: gfortran vvv_flx2mag_new.f -o vvv_flx2mag -Lcfitsio -lcfitsio -lm
cc    Compile on VVV server as: gfortran vvv_flx2mag.f -o vvv_flx2mag -L/sw/lib -lcfitsio -lm
cc
cc    This is a modified version of the code fitsio_cat_list written by CASU.
cc    This code computes the magnitudes and their errors in the first 7 apertures.
cc
cc    Written by Istvan Dekany
cc    idekany (a t) astro (d o t) puc (d o t) cl
cc
      parameter (ncmax=80,nrmax=2000000,napers=7)
      real*8 tpa,tpd,tand,secd,tpi,pi
      real*8 xx,yy,xi,xn
      real*8 flux(napers),fluxerr(napers),fixed(napers)
      real*4 rec(nrmax,ncmax)
      real*4 magzpt,magzrr,maglim
      real*8 magobj(napers),magerr(napers)
      real*4 bin(35,35)
      real*4 apcor(napers)
      real*4 delapcor(napers)
      character*80 infile,outfile
      character*80 argv,errmsg,dateobs,utstart,tablename
      character*72 comment
      character*68 value
      character*16 ttype(ncmax),tunit(ncmax),tform(ncmax)
      character*11 filter
      character*1 csign,czero
      integer status,blocksize
      logical first,illum
      common /illum/ first,bin
      common /rd/ tpa,tpd,tand,secd,tpi,a,b,c,d,e,f,projp1,projp3,
     1            projp5,iwcstype

      li=3
      lr=5
      lt=6
      pi=4.0*atan(1.0)
      tpi=8.0*atan(1.0)
      czero='0'
      first=.true.
      illum=.false.                         ! default is no illumination table

c *** try for presence of command line arguments
      m=iargc()
c *** if correct number of arguments try and interpret them
      if(m.eq.2.or.m.eq.3)then
        call getarg(1,argv)
        infile=argv
        call getarg(2,argv)
        outfile=argv
        if(m.eq.3)then
          call getarg(3,argv)
          tablename=argv
          illum=.true.
        endif
      else
        print *,' '
        print *,'Arguments: input output [illum. corr. table]'
        print *,' '
        stop
      endif
c *** open fits file and read in catalogue info
      iunit=1
      status=0
      call ftopen(iunit,infile,0,blocksize,status)        ! 0 readonly option
      if(status.ne.0)then
        call ftgerr(status,errmsg)
        print *,'FITSIO Error status =',status,': ',errmsg
        ier=1
        status=0
        call ftclos(iunit,status)
      endif
      call ftthdu(iunit,nhdus,status)
      nmefs=max(1,nhdus-1)

c *** clobber output file if exists
      call clobber(3,outfile)
      open(unit=3,file=outfile,status='new')

c *** see if VIRCAM or VST and get some info from PHU
      iesocam=0
      call ftgkey(iunit,'INSTRUME',value,comment,status)
      if(status.eq.202)then
        status=0
      else
        if(index(value,'VIRCAM').gt.0)iesocam=1
        if(index(value,'OMEGACAM').gt.0)iesocam=2
        if(iesocam.ne.0)then
          call ftgkys(iunit,'ESO INS FILT1 NAME',filter,comment,
     1                status)
          call ftgkye(iunit,'ESO TEL AIRM END',airmass,comment,
     1                status)
          call ftgcrd(iunit,'DATE-OBS',dateobs,status)
          utstart='UTST    ='
c *** try and read the exptime from here as a precaution
          call ftgkye(iunit,'EXPTIME',expsave,comment,status)
          if(status.eq.202)then
            expsave=0.0
            status=0
          endif
        endif
      endif

c *** now go through each extension
      do mef=1,nmefs
      call fitsio_rtable(rec,iunit,mef,nrows,ncols,apcor,delapcor,
     1  apcorpk,flim,filter,exptime,utstart,dateobs,percorr,magzpt,
     2  magzrr,airmass,extinct,skyvar,gain,saturate,pltscl,theta,
     3  iesocam)

c *** check for sensible exptime value
      if(exptime.eq.0.0)then
        exptime=expsave                                   ! see if PHU value ok
        if(exptime.eq.0.0)then
          print *,'exposure time information missing'
          stop
        endif
      endif

c *** some magnitude info
      magzpt=magzpt-(airmass-1.0)*extinct+2.5*log10(exptime)
      maglim=magzpt-2.5*log10(flim)                       ! 5-sigma mag limit

c *** print out photometry into ASCII catalog
      do k=1,nrows

      xcord=rec(k,3)
      ycord=rec(k,5)
      iflcol=20    ! column number of Aper_flux_1
      do iap=1,napers
      flux(iap)=max(0.1,rec(k,iflcol))
      fluxerr(iap)=max(1.0,rec(k,iflcol+1))
      iflcol=iflcol+2
      end do
      peak=rec(k,18)        ! peak flux
      peakerr=rec(k,19)     ! peak flux error
      skyloc=rec(k,56)      ! local sky
      tpeak=peak+skyloc
      confidence=rec(k,58)  ! average confidence level within default rcore aperture
      errbit=rec(k,55)      ! bit pattern listing various processing error flags
                            ! initially set to the number of bad pixels within radius 'rcore'
      ellipt=rec(k,8)
      pa=rec(k,9)-theta                                 ! correct to sky PA
      pa=90.0-pa                                        ! don't ask
c      if(pa.gt.180.0)pa=pa-180.0
      icls=rec(k,61)

c *** flag possibly saturated objects and correct flux for saturation
      ! Ring apertures are used for avoiding flux bias in PSF core:
      ! F = (f2 - f1) * C  ,
      ! C = c1c2/(c1-c2) = c1 / (c1/c2 - 1) = c1 / (delapcor - 1) ,
      ! where F is the corrected flux of a star, f1, f2 are fluxes in ap1 and ap2 (R_ap1 < R_ap2),
      ! and F = f1*c1 , F = f2*c2  (by definition)
      !
      if(tpeak.gt.saturate) then
      icls=-9                                                ! saturated ?
      ! Saturation correction using ring apertures
      ! AP7:
      fixed(7)=(flux(7)-flux(3))/(delapcor(7)-1.0)           ! flux in the ring between AP7 and AP3
      fixed(7)=fixed(7)*apcor(3)
      flux(7)=max(fixed(7),flux(7))
      ! AP6:
      fixed(6)=(flux(6)-flux(3))/(delapcor(6)-1.0)           ! flux in the ring between AP6 and AP3
      fixed(6)=fixed(6)*apcor(3)
      flux(6)=max(fixed(6),flux(6))
      ! AP5:
      fixed(5)=(flux(5)-flux(3))/(delapcor(5)-1.0)           ! flux in the ring between AP5 and AP3
      fixed(5)=fixed(5)*apcor(3)
      flux(5)=max(fixed(5),flux(3))
      ! AP4:
      fixed(4)=(flux(4)-flux(3))/(delapcor(4)-1.0)           ! flux in the ring between AP4 and AP2
      fixed(4)=fixed(4)*apcor(3)
      flux(4)=max(fixed(4),flux(4))
      ! AP3:
      fixed(3)=(flux(3)-flux(2))/(delapcor(3)-1.0)           ! flux in the ring between AP3 and AP2
      fixed(3)=fixed(3)*apcor(2)
      flux(3)=max(fixed(3),flux(3))
      ! AP2:
      fixed(2)=(flux(2)-flux(1))/(delapcor(2)-1.0)           ! flux in the ring between AP2 and AP1
      fixed(2)=fixed(2)*apcor(1)
      flux(2)=max(fixed(2),flux(2))
      ! AP1:
      fixed(1)=(flux(1)-peak)/(delapcor(1)-1.0)              ! flux in the 'ring' between AP1 and PEAK
      fixed(1)=fixed(1)*apcorpk
      flux(1)=max(fixed(1),flux(1))
      endif
c *** flag objects containing bad pixels
      if(errbit.gt.0.0)icls=-7

c *** a bit of astrometry
      xx=xcord
      yy=ycord
      call radeczp(xx,yy,xi,xn)
      call rahour(xx,ih,im,ss)
      call radeg(yy,id,in,rr)
      if(yy.lt.0)then
        csign='-'
        id=abs(id)
        in=abs(in)
        rr=abs(rr)
      else
        csign='+'
      endif

c *** apply aperture and distortion corrections
      call distort(xcord,ycord,distortcorr)                 ! field distortion correction
      do iap=1,napers
      if(icls.eq.-9) then
        flux(iap)=flux(iap)*percorr*distortcorr               ! ap. corr. already applied (see above)
      else
        flux(iap)=flux(iap)*apcor(iap)*percorr*distortcorr    ! aperture correction
      end if
      fluxerr(iap)=fluxerr(iap)*apcor(iap)*percorr*distortcorr
      magobj(iap)=magzpt-2.5*log10(flux(iap))                    ! object magnitude
      magerr(iap)=2.5*log10(1.0+fluxerr(iap)/flux(iap))
c      magerr=max(0.01,magerr)                          ! allow for systematics
      end do

c *** apply illumination correction table if required
      if(illum)then
        xi=xi*180.0/pi                                 ! xi,xn in deg
        xn=xn*180.0/pi
        do iap=1,napers
        call illumcor(tablename,xi,xn,magobj(iap))
        end do
      endif

c *** write results to output
      if(id.lt.10)then
        if(flux(3).ge.0.5)then                      ! only detected objects
        write(3,'(2i3,f8.4,1x,2a,i1,i3,f7.3,2f9.2,14f16.3,2i3,2f7.2,
     1  f10.4)')
     2  ih,im,ss,csign,czero,id,in,rr,xcord,ycord,
     3  magobj(1),magerr(1),magobj(2),magerr(2),magobj(3),magerr(3),
     4  magobj(4),magerr(4),magobj(5),magerr(5),magobj(6),magerr(6),
     5  magobj(7),magerr(7),mef,icls,ellipt,pa,confidence
        endif
      else
        if(flux(3).ge.0.5)then
        write(3,'(2i3,f8.4,1x,a,i2,i3,f7.3,2f9.2,14f16.3,2i3,2f7.2,
     1  f10.4)')
     2  ih,im,ss,csign,id,in,rr,xcord,ycord,
     3  magobj(1),magerr(1),magobj(2),magerr(2),magobj(3),magerr(3),
     4  magobj(4),magerr(4),magobj(5),magerr(5),magobj(6),magerr(6),
     5  magobj(7),magerr(7),mef,icls,ellipt,pa,confidence
        endif
      endif

      enddo                                            ! nrows loop
      enddo                                            ! mef loop

c *** and finish off
      call ftclos(iunit,status)
      if(status.ne.0)then
        call ftgerr(status,errmsg)
        print *,'FITSIO Error status =',status,': ',errmsg
      endif
      end

c *** -----------------------------------------------------------------------

      subroutine fitsio_rtable(rec,iunit,mef,nrows,ncols,apcor,delapcor,
     1 apcorpk,flim,filter,exptime,utstart,dateobs,percorr,magzpt,
     2 magzrr,airmass,extinct,skyvar,gain,saturate,pltscl,theta,iesocam)
      parameter (ncmax=80,nrmax=2000000,napers=7)
      real*8 tpa,tpd,tand,secd,tpi,pi
      real*4 rec(nrmax,ncmax)
      real*4 apcor(napers)
      real*4 delapcor(napers)
      integer status,hdutype,naxes(2)
      real*4 enullval,magzpt,magzrr
      character*80 errmsg,utstart,dateobs
      character*72 comment
      character*68 value
      character*16 ttype(ncmax),tunit(ncmax),tform(ncmax)
      character*11 filter
      character*1 ciap
      logical anynull
      common /rd/ tpa,tpd,tand,secd,tpi,a,b,c,d,e,f,projp1,projp3,
     1            projp5,iwcstype
c *** FITSIO_RTABLE - reads fits binary tables
      pi=4.0*atan(1.0)
      anynull=.false.
      enullval=0.0
c *** find out what sort of table extension it is
      status=0
      call ftmahd(iunit,mef+1,hdutype,status)
      if(hdutype.eq.1)then
        print *,'Extension is an ASCII table'
        stop ' - not implemented yet'
      else if(hdutype.eq.2)then
        print *,'Extension is a BINARY table'
      endif
c *** read assorted keywords to find table dimensions etc.
      call ftgknj(iunit,'NAXIS',1,2,naxes,nfound,status)
      if(nfound.ne.2)stop 'Not enough NAXIS keywords'
      ncols=naxes(1)/4                                    ! bytes to r*4
      nrows=naxes(2)
      print *,' '
      print *,'No. of columns =',ncols
      print *,'No. of rows    =',nrows
      if(nrows.gt.nrmax)then
        print *, ' '
        print *, 'nrmax limit exceeded'
        print *, ' '
        stop
      endif
      if(iesocam.eq.0)then
        call ftgkys(iunit,'WFFBAND',filter,comment,status)
        if(status.eq.202)then
          status=0
          call ftgkys(iunit,'FILTER',filter,comment,status)
          if(status.eq.202)then
            status=0
            call ftgkys(iunit,'HIERARCH ESO INS FILT1 NAME',
     1                  filter,comment,status)
          endif
        endif
        if(status.eq.202)then
          status=0
          filter='         '
        endif
      endif

      call ftgkye(iunit,'EXPTIME',exptime,comment,status)
      if(status.eq.202)then
        status=0
        call ftgkye(iunit,'EXPOSED',exptime,comment,status)
        if(status.eq.202)then
          status=0
          call ftgkye(iunit,'EXP_TIME',exptime,comment,status)
        endif
      endif
      if(status.eq.202)then
        status=0
        exptime=0.0
      else
        exptime=abs(exptime)
        exptime=max(1.0,exptime)
      endif

      call ftgkye(iunit,'SKYLEVEL',skylevel,comment,status)
      if(status.eq.202)then
        status=0.0
        call ftgkye(iunit,'ESO DRS SKYLEVEL',skylevel,comment,status)
        if(status.eq.202)then
          status=0
          skylevel=0.0
        endif
      endif

      call ftgkye(iunit,'SKYNOISE',skynoise,comment,status)
      if(status.eq.202)then
        status=0
        call ftgkye(iunit,'ESO DRS SKYNOISE',skynoise,comment,status)
        if(status.eq.202)then
          status=0
          skynoise=1.0
        endif
      endif

      call ftgkye(iunit,'RCORE',rcore,comment,status)
      if(status.eq.202)then
        status=0
        call ftgkye(iunit,'ESO DRS RCORE',rcore,comment,status)
        if(status.eq.202)then
          status=0
          rcore=1.0
        endif
      endif

      flim=5.0*sqrt(pi*rcore**2)*skynoise           ! 5-sigma flux limit
      skyvar=(pi*rcore**2)*skynoise**2              ! for error analysis

      call ftgkye(iunit,'GAIN',gain,comment,status)
      if(status.eq.202)then
        status=0
        gain=2.0                                    ! default
      endif
      if(iesocam.eq.1)gain=4.0
      if(iesocam.eq.2)gain=2.0

      call ftgkye(iunit,'SATURATE',saturate,comment,status)
      if(status.eq.202)then
        status=0
        saturate=30000.0                            ! safe guess
      endif
      print *,'SATURATE =',saturate                 ! average saturation level in frame
      saturate=0.9*saturate

      if(status.ne.0)then
        call ftgerr(status,errmsg)
        print *,'FITSIO Error status =',status,': ',errmsg
      endif

      call ftgkye(iunit,'APCORPK',apcorpk,comment,status)
      if(status.eq.202)then
          status=0
          apcorpk=0.0  ! default is no aperture correction
      endif
      apcorpk=10.0d0**(0.4d0*apcorpk)

      do iap=1,napers
        write(ciap,'(i1)') iap
        call ftgkye(iunit,'APCOR'//ciap,apcor(iap),comment,status)
        if(status.eq.202)then
          status=0
          apcor(iap)=0.0  ! default is no aperture correction
        endif
        print *,'APCOR'//ciap,'=',apcor(iap)
      end do
      ! Aperture corrections in flux for ring apertures
      delapcor(1)=10.0d0**(0.4d0*(apcorpk -apcor(1)))  ! 'ring' between ap1 and peak flux
      delapcor(2)=10.0d0**(0.4d0*(apcor(1)-apcor(2)))  ! ring between ap2 and ap1
      delapcor(3)=10.0d0**(0.4d0*(apcor(2)-apcor(3)))  ! ring between ap3 and ap2
      delapcor(4)=10.0d0**(0.4d0*(apcor(3)-apcor(4)))  ! ring between ap3 and ap4
      delapcor(5)=10.0d0**(0.4d0*(apcor(3)-apcor(5)))  ! ring between ap3 and ap5
      delapcor(6)=10.0d0**(0.4d0*(apcor(3)-apcor(6)))  ! ring between ap3 and ap6
      delapcor(7)=10.0d0**(0.4d0*(apcor(3)-apcor(7)))  ! ring between ap3 and ap7
      ! Aperture corrections in flux for apertures 1-7
      do iap=1,napers
        apcor(iap)=10.0d0**(0.4d0*apcor(iap))
      end do

      call ftgkye(iunit,'PERCORR',percorr,comment,status)
      if(status.eq.202)then
        status=0
        percorr=0.0
      endif
      percorr=10.0**(0.4*percorr)

      flim=flim*apcor(3)*percorr                    ! including ap and per corr

      if(iesocam.eq.0)then
        call ftgkye(iunit,'AIRMASS',airmass,comment,status)
        if(status.eq.202)then
          status=0
          call ftgkye(iunit,'AMSTART',airmass1,comment,status)
          call ftgkye(iunit,'AMEND',airmass2,comment,status)
          if(status.eq.202)then
            status=0
            airmass=1.0
          else
            airmass=0.5*(airmass1+airmass2)
          endif
        endif
      endif

      call ftgkye(iunit,'MAGZPT',magzpt,comment,status)
      if(status.eq.202)then
        status=0
        magzpt=-1.0
      endif

      call ftgkye(iunit,'MAGZRR',magzrr,comment,status)
      if(status.eq.202)then
        status=0
        magzrr=-1.0
      endif

c *** get extinction value - if not present use default
      call ftgkye(iunit,'EXTINCT',extinct,comment,status)
      if(status.eq.202)then
        status=0
        extinct=0.05
      endif

c *** check if WCS information present
      call ftgkey(iunit,'CTYPE1',value,comment,status)
      if(status.eq.202)then
        status=0
        call ftgkey(iunit,'TCTYP3',value,comment,status)
      endif
      if(status.ne.202)then
        if(index(value,'RA---').gt.0)then           ! need a better test
          iwcs=1
        else
          iwcs=0
        endif
      else
        iwcs=0
        status=0
      endif
      if(iwcs.eq.1)then
        print *,'Valid WCS present'
      else
        print *,'No WCS present'
      endif
c *** now get celestial coordinate information the hard way
      if(iwcs.eq.1)then
        iwcstype=1                                             ! ZPN default
        if(iesocam.eq.0)then
          call ftgkys(iunit,'CTYPE1',value,comment,status)
          if(index(value,'RA---TAN').gt.0)iwcstype=0           ! TAN only other
          call ftgkye(iunit,'CRPIX1',c,comment,status)
          call ftgkye(iunit,'CRPIX2',f,comment,status)
          call ftgkyd(iunit,'CRVAL1',tpa,comment,status)
          call ftgkyd(iunit,'CRVAL2',tpd,comment,status)
        else
          call ftgkys(iunit,'TCTYP3',value,comment,status)
          if(index(value,'RA---TAN').gt.0)iwcstype=0           ! TAN only other
          call ftgkye(iunit,'TCRPX3',c,comment,status)
          call ftgkye(iunit,'TCRPX5',f,comment,status)
          call ftgkyd(iunit,'TCRVL3',tpa,comment,status)
          call ftgkyd(iunit,'TCRVL5',tpd,comment,status)
        endif
        tpa=tpa*pi/180.0
        tpd=tpd*pi/180.0
        tand=tan(tpd)
        secd=1.0/cos(tpd)
        call ftgkye(iunit,'CD1_1',a,comment,status)
        if(status.eq.202)then
          status=0
          call ftgkye(iunit,'TC3_3',a,comment,status)
        endif
        a=a*pi/180.0
        call ftgkye(iunit,'CD2_2',e,comment,status)
        if(status.eq.202)then
          status=0
          call ftgkye(iunit,'TC5_5',e,comment,status)
        endif
        e=e*pi/180.0
        call ftgkye(iunit,'CD1_2',b,comment,status)
        if(status.eq.202)then
          status=0
          call ftgkye(iunit,'TC3_5',b,comment,status)
          if(status.eq.202)then
            status=0
            b=0.0
          endif
        endif
        b=b*pi/180.0
        call ftgkye(iunit,'CD2_1',d,comment,status)
        if(status.eq.202)then
          status=0
          call ftgkye(iunit,'TC5_3',d,comment,status)
          if(status.eq.202)then
            status=0
            d=0.0
          endif
        endif
        d=d*pi/180.0
        if(iverbose.eq.1)then
          print *,' '
          print *,'Frame transform constants:'
          print *,a,b,c
          print *,d,e,f
          print *,'Tangent points:',tpa,tpd
        endif
c *** pixel scale and orientation
        pltscl=sqrt(0.5*(a**2+b**2+d**2+e**2))*180.0/pi               ! deg/pix
        theta=0.5*(atan(abs(b)/abs(a))+atan(abs(d)/abs(e)))*180.0/pi  ! deg
c *** read zp constants from header
        projp1=1.0
        call ftgkye(iunit,'PV2_3',projp3,comment,status)
        if(status.eq.202)then
          status=0
          call ftgkye(iunit,'PROJP3',projp3,comment,status)
          if(status.eq.202)then
            status=0
            call ftgkye(iunit,'TV5_3',projp3,comment,status)
            if(status.eq.202)then
              status=0
              projp3=0.0
            endif
          endif
        endif
        call ftgkye(iunit,'PV2_5',projp5,comment,status)
        if(status.eq.202)then
          status=0
          call ftgkye(iunit,'TV5_5',projp5,comment,status)
          if(status.eq.202)then
            status=0
            projp5=0.0
          endif
        endif
      endif
c *** observation time and date
      if(iesocam.eq.0)then
        call ftgcrd(iunit,'UTSTART',utstart,status)
        if(status.eq.202)then
          status=0
          call ftgcrd(iunit,'UTC-OBS',utstart,status)
        endif
        if(status.eq.202)then
          utstart='UTST    ='
          status=0
        endif
        call ftgcrd(iunit,'DATE-OBS',dateobs,status)
        if(status.eq.202)then
          dateobs='DATE    ='
          status=0
        endif
      endif
      if(status.ne.0)then
        call ftgerr(status,errmsg)
        print *,'FITSIO Error status =',status,': ',errmsg
      endif
c *** now get rest of table info
      call ftgkns(iunit,'TTYPE',1,ncmax,ttype,nfound,status)
      call ftgkns(iunit,'TUNIT',1,ncmax,tunit,nfound,status)
      call ftgkns(iunit,'TFORM',1,ncmax,tform,nfound,status)
      if(ncols.ne.nfound)stop 'Something screwy with header'
      if(iverbose.eq.1)then
        print *,' '
        print *,'Parameters in table:'
        print *,(ttype(i),i=1,ncols)
        print *,' '
      endif
c *** now read data column by column (yuk!)
      do j=1,ncols
      call ftgcve(iunit,j,1,1,nrows,enullval,rec(1,j),anynull,status)
      enddo
c *** check for errors
      if(status.ne.0)then
        call ftgerr(status,errmsg)
        print *,'FITSIO Error status =',status,': ',errmsg
      endif
      return
      end

c *** -----------------------------------------------------------------------

      subroutine radeczp(x,y,xi,xn)
      real*8 tpa,tpd,tand,secd,tpi
      real*8 xi,xn,aa,alpha,delta,x,y
      common /rd/ tpa,tpd,tand,secd,tpi,a,b,c,d,e,f,projp1,projp3,
     1            projp5,iwcstype
c *** RADECZP converts x,y coordinates to ra and dec using ZP wrt optical axis
c *** NB. This is an extension of the ARC projection using Zenithal polynomials
      x=x-c
      y=y-f
      xi=a*x+b*y
      xn=d*x+e*y
      r=sqrt(xi**2+xn**2)
      if(iwcstype.eq.1)then
        rfac=projp1+projp3*r**2+projp5*r**4    ! NB this is a 1st order approx
        r=r/rfac
        rfac=projp1+projp3*r**2+projp5*r**4    ! now 2nd order correction
      else
        if(r.eq.0.0)then
          rfac=1.0
        else
          rfac=r/tan(r)
        endif
      endif
      xi=xi/rfac
      xn=xn/rfac
      aa=atan(xi*secd/(1.0-xn*tand))
      alpha=aa+tpa
      delta=atan((xn+tand)*sin(aa)/(xi*secd))
      x=alpha
      y=delta
      if(x.gt.tpi)x=x-tpi
      if(x.lt.0.0)x=x+tpi
      return
      end

c *** -----------------------------------------------------------------------

      subroutine distort(x,y,distortcorr)
      real*8 tpa,tpd,tand,secd,tpi
      common /rd/ tpa,tpd,tand,secd,tpi,a,b,c,d,e,f,projp1,projp3,
     1            projp5,iwcstype
c *** DISTORT converts x,y coordinates to standard coordinates
c ***         and works out flux distortion factor
c *** NB. This is an extension of the ARC projection using Zenithal polynomials
      xi=a*(x-c)+b*(y-f)
      xn=d*(x-c)+e*(y-f)
      r=sqrt(xi**2+xn**2)
      if(iwcstype.eq.1)then
        distortcorr=1.0+3.0*projp3*r**2/projp1+
     1                  5.0*projp5*r**4/projp1 ! this is only a 1st order app
        distortcorr=distortcorr*(1.0+(projp3*r**2+projp5*r**4)/projp1)
      else
        if(r.eq.0.0)then
          distortcorr=1.0
        else
          distortcorr=1.0/(1.0+tan(r)**2)
          distortcorr=distortcorr*atan(r)/r
        endif
      endif
      distortcorr=1.0/distortcorr              ! is this the right sign
      return                                   ! r.dr.dtheta --> r'dr'dtheta
      end

c *** -----------------------------------------------------------------------

      subroutine clobber(iunit,name)
c *** CLOBBER checks to see if file name exists and then deletes it
      integer ier,stat,statb(13),status,system
      character*(*) name
      character*80 filename
      character*3 del /'rm '/
      character*1 space /' '/
      filename=name
      i=1
      do while (filename(i:i).ne.' '.and.i.lt.80)
      i=i+1
      enddo
      open(unit=iunit,file=filename)
      ier=stat(filename,statb)
      if(statb(8).eq.0.or.ier.ne.0)then
        status=system(del//filename)
      else
        print *,'File: '//filename(1:i)//' already exists - clobbering'
        status=system(del//filename)
      endif
      close(unit=iunit)
      return
      end

c *** ------------------------------------------------------------------------

      subroutine rahour(radian,ihour,min,sec)
      real*8 radian,temp
      character*6 dum
c *** RAHOUR  converts radians into hours,mins,secs
      tpi=8.0*atan(1.0)
      if(radian.lt.0.0)radian=radian+tpi
c *** convert to secs
      temp=10800.0*abs(radian)/atan(1.0)
      ir=int(temp)
      ihour=ir/3600
      min=(ir-3600*ihour)/60
      sec=(temp-ihour*3600-min*60)

c *** check for illegal sexagesimals
      write(dum,'(f6.2)') sec
      if(index(dum,'60.').ne.0)then
        sec=0.0
        min=min+1
      endif
      if(min.eq.60)then
        min=0
        ihour=ihour+1
        if(ihour.eq.24)ihour=0
      endif

      return
      end

c *** ------------------------------------------------------------------------

      subroutine radeg(radian,ideg,min,sec)
      real*8 radian,temp
      character*5 dum
c *** RADEG  converts radians into degs,mins,secs
      iflag=1
      if(radian.lt.0.0)iflag=-1
c *** convert to secs
      temp=162000.0*abs(radian)/atan(1.0)
      ir=int(temp)
      ideg=ir/3600
      min=(ir-3600*ideg)/60
      sec=(temp-ideg*3600-min*60)*iflag

c *** check for illegal sexagesimals
      write(dum,'(f5.1)') sec
      if(index(dum,'60.').ne.0)then
        sec=0.0
        min=min+1
      endif
      if(min.eq.60)then
        min=0
        ideg=ideg+1
      endif

      min=min*iflag
      ideg=ideg*iflag
      return
      end

c *** ------------------------------------------------------------------------

      subroutine illumcor(tablename,xideg,xndeg,magobj)
      real*8 xideg,xndeg
      real*4 bin(35,35)
      real*8 magobj
      character*80 tablename
      logical first
      common /illum/ first,bin
c *** ILLUMCOR does illumination correction (currently only for WFCAM)
      if(first)then
c *** read in table
        open(unit=1,file=tablename,status='old')
        read(1,*)
        do j=1,35
        do i=1,35
        read(1,*) xi,xn,bin(i,j),numb
        enddo
        enddo
        close(unit=1)
        first=.false.
      endif
c *** use input xi,xn in deg to find appropriate closest illum corr
      i=int(xideg*50.0+18.0)
      delx=xideg*50.0+18.0-i
      j=int(xndeg*50.0+18.0)
      dely=xndeg*50.0+18.0-j
c *** bilinear interpolation
      if(magobj.ne.0.0)
     1 magobj=magobj+(1.0-dely)*((1.0-delx)*bin(i,j)+delx*bin(i+1,j))+
     2                   dely*((1.0-delx)*bin(i,j+1)+delx*bin(i+1,j+1))
      return
      end

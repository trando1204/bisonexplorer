import TurboQuery from '../helpers/turbolinks_helper'
import { requestJSON } from '../helpers/http'
import humanize from '../helpers/humanize_helper'
import FinanceReportController from './financebase_controller'

const responseCache = {}
let requestCounter = 0
let domainList
let tokenList
let ownerList
let responseData

function hasCache (k) {
  if (!responseCache[k]) return false
  const expiration = new Date(responseCache[k].expiration)
  return expiration > new Date()
}

export default class extends FinanceReportController {
  static get targets () {
    return ['noData', 'reportArea',
      'proposalReport', 'legacyReport', 'yearMonthInfoTable',
      'nextButton', 'proposalArea', 'noReport',
      'totalSpanRow', 'monthlyArea', 'yearlyArea',
      'monthlyReport', 'yearlyReport', 'summaryArea', 'summaryReport',
      'proposalSpanRow', 'prevBtn', 'nextBtn',
      'toVote', 'toDiscussion',
      'expendiduteValue', 'prevNextButtons', 'toUpReport',
      'currentDetail', 'yearBreadcumb', 'proposalSumCard', 'proposalTopSummary',
      'domainSummaryTable', 'domainSummaryArea', 'proposalSpent',
      'treasurySpent', 'unaccountedValue', 'proposalSpentArea', 'treasurySpentArea',
      'unaccountedValueArea', 'detailReportTitle', 'summaryTableTitle']
  }

  async initialize () {
    this.query = new TurboQuery()
    this.settings = TurboQuery.nullTemplate([
      'type', 'time', 'token', 'name', 'stype', 'order'
    ])

    this.politeiaUrl = this.data.get('politeiaUrl')

    this.defaultSettings = {
      type: '',
      time: '',
      token: '',
      name: '',
      stype: 'pname',
      order: 'desc'
    }

    if (!this.settings.order) {
      this.settings.order = this.defaultSettings.order
    }

    this.query.update(this.settings)

    if (!this.settings.type) {
      this.showNoData()
      return
    }

    if (this.settings.type === 'month' || this.settings.type === 'year') {
      if (!this.settings.time) {
        this.showNoData()
        return
      }
    }
    if (this.settings.type === 'domain' || this.settings.type === 'owner') {
      if (!this.settings.name) {
        this.showNoData()
        return
      }
    }
    if (this.settings.type === 'proposal' && !this.settings.token) {
      this.showNoData()
      return
    }
    this.noDataTarget.classList.add('d-none')
    this.reportAreaTarget.classList.remove('d-none')

    if (this.settings.type === 'proposal' || this.settings.type === 'domain' || this.settings.type === 'owner') {
      this.prevNextButtonsTarget.classList.add('d-none')
    } else {
      this.prevNextButtonsTarget.classList.remove('d-none')
    }
    if (this.settings.type === 'month' || this.settings.type === 'year') {
      this.yearMonthCalculate()
      return
    }
    this.initScrollerForTable()
    this.proposalTopSummaryTarget.classList.add('d-none')
    this.totalSpanRowTarget.classList.add('d-none')
    this.proposalCalculate()
  }

  async initScrollerForTable () {
    const $scroller = document.getElementById('scroller')
    const $container = document.getElementById('containerBody')
    const $wrapper = document.getElementById('wrapperReportTable')
    let ignoreScrollEvent = false
    let animation = null
    const scrollbarPositioner = () => {
      const scrollTop = document.scrollingElement.scrollTop
      const wrapperTop = $wrapper.offsetTop
      const wrapperBottom = wrapperTop + $wrapper.offsetHeight

      const topMatch = (window.innerHeight + scrollTop) >= wrapperTop
      const bottomMatch = (scrollTop) <= wrapperBottom

      if (topMatch && bottomMatch) {
        const inside = wrapperBottom >= scrollTop && window.innerHeight + scrollTop <= wrapperBottom

        if (inside) {
          $scroller.style.bottom = '0px'
        } else {
          const offset = (scrollTop + window.innerHeight) - wrapperBottom

          $scroller.style.bottom = offset + 'px'
        }
        $scroller.classList.add('visible')
      } else {
        $scroller.classList.remove('visible')
      }

      window.requestAnimationFrame(scrollbarPositioner)
    }

    window.requestAnimationFrame(scrollbarPositioner)

    $scroller.addEventListener('scroll', (e) => {
      if (ignoreScrollEvent) return false

      if (animation) window.cancelAnimationFrame(animation)
      animation = window.requestAnimationFrame(() => {
        ignoreScrollEvent = true
        $container.scrollLeft = $scroller.scrollLeft
        ignoreScrollEvent = false
      })
    })

    $container.addEventListener('scroll', (e) => {
      if (ignoreScrollEvent) return false

      if (animation) window.cancelAnimationFrame(animation)
      animation = window.requestAnimationFrame(() => {
        ignoreScrollEvent = true
        $scroller.scrollLeft = $container.scrollLeft

        ignoreScrollEvent = false
      })
    })

    $(window).on('resize', function () {
      // get table thead size
      const tableWidthStr = $('#reportTable thead').css('width').replace('px', '')
      const tableWidth = parseFloat(tableWidthStr.trim())
      const parentContainerWidthStr = $('#summaryTableArea').css('width').replace('px', '')
      const parentContainerWidth = parseFloat(parentContainerWidthStr.trim())
      if (tableWidth < parentContainerWidth + 5) {
        $('#scroller').addClass('d-none')
      } else {
        $('#scroller').removeClass('d-none')
      }
      // set overflow class
      $('#scroller').css('width', $('#summaryTableArea').css('width'))
    })
  }

  showNoData () {
    this.noDataTarget.classList.remove('d-none')
    this.reportAreaTarget.classList.add('d-none')
  }

  updateQueryString () {
    const [query, settings, defaults] = [{}, this.settings, this.defaultSettings]
    for (const k in settings) {
      if (!settings[k] || settings[k].toString() === defaults[k].toString()) continue
      query[k] = settings[k]
    }
    this.query.replace(query)
  }

  prevReport (e) {
    let currentValue
    if (this.settings.type === 'domain' || this.settings.type === 'owner') {
      const itemIndex = this.settings.type === 'domain' ? domainList.indexOf(this.settings.name) : ownerList.indexOf(this.settings.name)
      if (itemIndex < 0) {
        return
      }
      this.settings.name = this.settings.type === 'domain' ? domainList[itemIndex - 1] : ownerList[itemIndex - 1]
      currentValue = this.settings.name
    }

    if (this.settings.type === 'proposal') {
      const itemIndex = tokenList.indexOf(this.settings.token)
      if (itemIndex < 0) {
        return
      }
      this.settings.token = tokenList[itemIndex - 1]
      currentValue = this.settings.token
    }

    if (this.settings.type === 'year') {
      this.settings.time = this.settings.time - 1
    } else if (this.settings.type === 'month') {
      const timeArr = this.settings.time.trim().split('_')
      let year = parseInt(timeArr[0])
      let month = parseInt(timeArr[1])
      if (month === 1) {
        year = year - 1
        month = 12
      } else {
        month = month - 1
      }
      this.settings.time = year + '_' + month
    }

    this.updateQueryString()
    if (this.settings.type === 'year' || this.settings.type === 'month') {
      this.yearMonthCalculate()
    }
    if (this.settings.type === 'domain' || this.settings.type === 'proposal' || this.settings.type === 'owner') {
      this.handlerNextPrevButton(this.settings.type, currentValue)
      this.proposalCalculate()
    }
  }

  nextReport (e) {
    let currentValue
    if (this.settings.type === 'domain' || this.settings.type === 'owner') {
      const itemIndex = this.settings.type === 'domain' ? domainList.indexOf(this.settings.name) : ownerList.indexOf(this.settings.name)
      if (itemIndex < 0) {
        return
      }
      this.settings.name = this.settings.type === 'domain' ? domainList[itemIndex + 1] : ownerList[itemIndex + 1]
      currentValue = this.settings.name
    }

    if (this.settings.type === 'proposal') {
      const itemIndex = tokenList.indexOf(this.settings.token)
      if (itemIndex < 0) {
        return
      }
      currentValue = this.settings.token
      this.settings.token = tokenList[itemIndex + 1]
    }
    if (this.settings.type === 'year') {
      this.settings.time = this.settings.time + 1
    } else if (this.settings.type === 'month') {
      const timeArr = this.settings.time.trim().split('_')
      let year = parseInt(timeArr[0])
      let month = parseInt(timeArr[1])
      if (month === 12) {
        year = year + 1
        month = 1
      } else {
        month = month + 1
      }
      this.settings.time = year + '_' + month
    }
    this.updateQueryString()

    if (this.settings.type === 'year' || this.settings.type === 'month') {
      this.yearMonthCalculate()
    }
    if (this.settings.type === 'domain' || this.settings.type === 'proposal' || this.settings.type === 'owner') {
      this.handlerNextPrevButton(this.settings.type, currentValue)
      this.proposalCalculate()
    }
  }

  proposalDetailsListUpdate () {
    if (this.settings.type === 'domain' || this.settings.type === 'owner') {
      this.summaryReportTarget.innerHTML = this.createSummaryTable(responseData.proposalInfos, this.settings.type === 'owner', this.settings.type === 'domain')
      this.handlerScrollerTable()
    } else if (this.settings.type === 'proposal') {
      this.summaryReportTarget.innerHTML = this.createSummaryTable(responseData.otherProposalInfos, true, false)
      this.handlerScrollerTable()
    }
  }

  yearMonthProposalListUpdate () {
    this.proposalReportTarget.innerHTML = this.createProposalDetailReport(responseData)
  }

  async proposalCalculate () {
    this.yearBreadcumbTarget.classList.add('d-none')
    if (this.settings.type === 'domain') {
      const domainDisp = this.settings.name.charAt(0).toUpperCase() + this.settings.name.slice(1)
      this.currentDetailTarget.textContent = domainDisp
      this.detailReportTitleTarget.textContent = 'Domain Detail Report - ' + domainDisp
      // set main report url
      this.toUpReportTarget.href = '/finance-report?pgroup=domains'
    } else if (this.settings.type === 'owner') {
      this.currentDetailTarget.textContent = this.settings.name
      this.detailReportTitleTarget.textContent = 'Author Detail Report - ' + this.settings.name
      this.toUpReportTarget.href = '/finance-report?pgroup=authors'
    } else {
      this.toUpReportTarget.href = '/finance-report'
    }
    if (this.settings.type === 'domain' || this.settings.type === 'proposal') {
      this.nextBtnTarget.classList.add('d-none')
      this.prevBtnTarget.classList.add('d-none')
    } else {
      this.prevBtnTarget.classList.remove('d-none')
      this.nextBtnTarget.classList.remove('d-none')
    }
    const url = `/api/finance-report/detail?type=${this.settings.type}&${this.settings.type === 'proposal' ? 'token=' + this.settings.token : 'name=' + this.settings.name}`
    let response
    requestCounter++
    const thisRequest = requestCounter
    if (hasCache(url)) {
      response = responseCache[url]
    } else {
      // response = await axios.get(url)
      response = await requestJSON(url)
      responseCache[url] = response
      if (thisRequest !== requestCounter) {
        // new request was issued while waiting.
        console.log('Response request different')
      }
    }
    if (!response) {
      this.monthlyAreaTarget.classList.add('d-none')
      this.yearlyAreaTarget.classList.add('d-none')
      if (this.settings.type === 'domain') {
        this.summaryAreaTarget.classList.add('d-none')
      }
      return
    }
    responseData = response
    this.monthlyAreaTarget.classList.remove('d-none')
    this.yearlyAreaTarget.classList.remove('d-none')
    if (this.settings.type === 'domain' || this.settings.type === 'owner') {
      this.summaryAreaTarget.classList.remove('d-none')
      this.summaryTableTitleTarget.textContent = 'Proposals'
      this.summaryReportTarget.innerHTML = this.createSummaryTable(response.proposalInfos, this.settings.type === 'owner', this.settings.type === 'domain')
      this.handlerScrollerTable()
      this.setDomainGeneralInfo(response, this.settings.type)
      if (this.settings.type === 'domain') {
        domainList = response.domainList
      } else {
        ownerList = response.ownerList
      }
      this.handlerNextPrevButton(this.settings.type === 'domain' ? 'domain' : 'owner', this.settings.name)
    }
    if (this.settings.type === 'proposal') {
      // show summary proposal list
      if (!response.otherProposalInfos || response.otherProposalInfos.length === 0) {
        this.summaryAreaTarget.classList.add('d-none')
      } else {
        this.summaryAreaTarget.classList.remove('d-none')
        this.summaryTableTitleTarget.textContent = 'Proposals with the same owner'
        this.summaryReportTarget.innerHTML = this.createSummaryTable(response.otherProposalInfos, true, false)
        this.handlerScrollerTable()
      }
      this.toVoteTarget.classList.remove('d-none')
      this.toDiscussionTarget.classList.remove('d-none')
      this.toVoteTarget.href = `/proposal/${this.settings.token}`
      this.toDiscussionTarget.href = `${this.politeiaUrl}/record/${this.settings.token.substring(0, 7)}`
      tokenList = response.tokenList
      this.handlerNextPrevButton('proposal', this.settings.token)
      const proposalName = response.proposalInfo ? response.proposalInfo.name : ''
      this.currentDetailTarget.textContent = proposalName
      this.detailReportTitleTarget.textContent = 'Proposal Detail Report - ' + proposalName
      this.proposalSumCardTarget.classList.remove('d-none')
      const remainingStr = response.proposalInfo.totalRemaining === 0.0 ? '<p>Status: <span class="fw-600">Finished</span></p>' : `<p>Total Remaining (Est): <span class="fw-600">$${humanize.formatToLocalString(response.proposalInfo.totalRemaining, 2, 2)}</span></p>`
      this.proposalSpanRowTarget.innerHTML = `<p>Owner: <a href="${'/finance-report/detail?type=owner&name=' + response.proposalInfo.author}" class="fw-600 link-hover-underline">${response.proposalInfo.author}</a></p>` +
      `<p>Domain: <a href="${'/finance-report/detail?type=domain&name=' + response.proposalInfo.domain}" class="link-hover-underline fw-600">${response.proposalInfo.domain.charAt(0).toUpperCase() + response.proposalInfo.domain.slice(1)}</a></p>` +
      `<p>Start Date: <span class="fw-600">${response.proposalInfo.start}</span></p>` +
      `<p>End Date: <span class="fw-600">${response.proposalInfo.end}</span></p>` +
      `<p>Budget: <span class="fw-600">$${humanize.formatToLocalString(response.proposalInfo.budget, 2, 2)}</span></p>` +
      `<p>Total Spent (Est): <span class="fw-600">$${humanize.formatToLocalString(response.proposalInfo.totalSpent, 2, 2)}</span></p>` + remainingStr
    } else {
      this.toVoteTarget.classList.add('d-none')
      this.toDiscussionTarget.classList.add('d-none')
    }
    // Create info of
    // create monthly table
    if (this.settings.type === 'owner') {
      this.monthlyAreaTarget.classList.add('d-none')
      this.yearlyAreaTarget.classList.add('d-none')
    } else {
      if (this.settings.type === 'proposal') {
        // if proposal, hide yearly summary
        this.yearlyAreaTarget.classList.add('d-none')
      } else {
        this.yearlyAreaTarget.classList.remove('d-none')
        this.yearlyReportTarget.innerHTML = this.createMonthYearTable(response, 'year')
      }
      this.monthlyAreaTarget.classList.remove('d-none')
      this.monthlyReportTarget.innerHTML = this.createMonthYearTable(response, 'month')
    }
  }

  handlerScrollerTable () {
    const tableWidthStr = $('#reportTable thead').css('width').replace('px', '')
    const tableWidth = parseFloat(tableWidthStr.trim())
    const parentContainerWidthStr = $('#summaryTableArea').css('width').replace('px', '')
    const parentContainerWidth = parseFloat(parentContainerWidthStr.trim())
    let hideScroller = false
    if (tableWidth < parentContainerWidth + 5) {
      $('#scroller').addClass('d-none')
      hideScroller = true
    } else {
      $('#scroller').removeClass('d-none')
    }
    this.summaryReportTarget.classList.add('proposal-table-padding')
    $('#reportTable').css('width', 'auto')
    $('html').css('overflow-x', 'hidden')
    // set overflow class
    $('#containerReportTable').addClass('of-x-hidden')
    $('#containerBody').addClass('of-x-hidden')
    $('#scrollerLong').css('width', (tableWidth + 25) + 'px')
    // set scroller width fit with container width
    $('#scroller').css('width', $('#summaryTableArea').css('width'))
    if (this.isMobile()) {
      $('#containerBody').css('overflow', 'scroll')
      this.summaryReportTarget.classList.remove('proposal-table-padding')
      $('#scroller').addClass('d-none')
    } else {
      this.summaryReportTarget.classList.add('proposal-table-padding')
      if (!hideScroller) {
        $('#scroller').removeClass('d-none')
      }
    }
  }

  setDomainGeneralInfo (data, type) {
    this.proposalSumCardTarget.classList.remove('d-none')
    let totalBudget = 0; let totalSpent = 0; let totalRemaining = 0
    if (data.proposalInfos && data.proposalInfos.length > 0) {
      data.proposalInfos.forEach((proposal) => {
        totalBudget += proposal.budget
        totalSpent += proposal.totalSpent
        totalRemaining += proposal.totalRemaining > 0 ? proposal.totalRemaining : 0
      })
    }
    this.proposalSpanRowTarget.innerHTML = `<p>Total Budget: <span class="fw-600">$${humanize.formatToLocalString(totalBudget, 2, 2)}</span></p>` +
    `<p>Total ${type === 'owner' ? 'Received' : 'Spent'} (Estimate):<span class="fw-600">$${humanize.formatToLocalString(totalSpent, 2, 2)}</span></p>` +
    `<p>Total Remaining (Estimate): <span class="fw-600">$${humanize.formatToLocalString(totalRemaining, 2, 2)}</span></p>`
  }

  handlerNextPrevButton (type, currentValue) {
    let handlerList
    if (type === 'domain') {
      handlerList = domainList
    } else if (type === 'proposal') {
      handlerList = tokenList
    } else if (type === 'owner') {
      handlerList = ownerList
    }

    if (!handlerList || handlerList.length < 1) {
      return
    }
    const indexOfNow = handlerList.indexOf(currentValue)
    if (indexOfNow < 0) {
      return
    }
    if (indexOfNow === 0) {
      // disable left array button
      this.prevBtnTarget.classList.add('disabled')
      this.prevBtnTarget.classList.remove('cursor-pointer')
    } else {
      this.prevBtnTarget.classList.remove('disabled')
      this.prevBtnTarget.classList.add('cursor-pointer')
    }
    if (indexOfNow === handlerList.length - 1) {
      this.nextBtnTarget.classList.add('disabled')
      this.nextBtnTarget.classList.remove('cursor-pointer')
    } else {
      this.nextBtnTarget.classList.remove('disabled')
      this.nextBtnTarget.classList.add('cursor-pointer')
    }
  }

  handlerYearMonthNextPrevButton () {
    const time = this.settings.time
    let prevBtnShow = true
    let nextBtnShow = true
    if (this.settings.type === 'year') {
      if (time === this.reportMinYear) {
        prevBtnShow = false
      }
      if (time === this.reportMaxYear) {
        nextBtnShow = false
      }
    } else if (this.settings.type === 'month') {
      const timeArr = this.settings.time.trim().split('_')
      const year = parseInt(timeArr[0])
      const month = parseInt(timeArr[1])
      if (year === this.reportMinYear && month === this.reportMinMonth) {
        prevBtnShow = false
      }
      if (year === this.reportMaxYear && month === this.reportMaxMonth) {
        nextBtnShow = false
      }
    }
    if (prevBtnShow) {
      this.prevBtnTarget.classList.remove('disabled')
      this.prevBtnTarget.classList.add('cursor-pointer')
    } else {
      this.prevBtnTarget.classList.add('disabled')
      this.prevBtnTarget.classList.remove('cursor-pointer')
    }
    if (nextBtnShow) {
      this.nextBtnTarget.classList.remove('disabled')
      this.nextBtnTarget.classList.add('cursor-pointer')
    } else {
      this.nextBtnTarget.classList.add('disabled')
      this.nextBtnTarget.classList.remove('cursor-pointer')
    }
  }

  createSummaryTable (data, hideAuthor, hideDomain) {
    $('#reportTable').css('width', '')
    if (!data) {
      return ''
    }
    let thead = '<thead>' +
      '<tr class="text-secondary finance-table-header">' +
      '<th class="va-mid text-center month-col fs-13i fw-600 proposal-name-col"><label class="cursor-pointer" data-action="click->financedetail#sortByPName">Name</label>' +
      `<span data-action="click->financedetail#sortByPName" class="${(this.settings.stype === 'pname' && this.settings.order === 'desc') ? 'dcricon-arrow-down' : 'dcricon-arrow-up'} ${this.settings.stype !== 'pname' ? 'c-grey-4' : ''} col-sort ms-1"></span></th>`
    if (!hideDomain) {
      thead += '<th class="va-mid text-center fs-13i px-2 fw-600"><label class="cursor-pointer" data-action="click->financedetail#sortByDomain">Domain</label>' +
        `<span data-action="click->financedetail#sortByDomain" class="${(this.settings.stype === 'domain' && this.settings.order === 'desc') ? 'dcricon-arrow-down' : 'dcricon-arrow-up'} ${this.settings.stype !== 'domain' ? 'c-grey-4' : ''} col-sort ms-1"></span></th>`
    }
    if (!hideAuthor) {
      thead += '<th class="va-mid text-center fs-13i px-2 fw-600"><label class="cursor-pointer" data-action="click->financedetail#sortByAuthor">Author</label>' +
        `<span data-action="click->financedetail#sortByAuthor" class="${(this.settings.stype === 'author' && this.settings.order === 'desc') ? 'dcricon-arrow-down' : 'dcricon-arrow-up'} ${this.settings.stype !== 'author' ? 'c-grey-4' : ''} col-sort ms-1"></span></th>`
    }
    thead += '<th class="va-mid text-center fs-13i px-2 fw-600"><label class="cursor-pointer" data-action="click->financedetail#sortByStartDate">Start Date</label>' +
      `<span data-action="click->financedetail#sortByStartDate" class="${(this.settings.stype === 'startdt' && this.settings.order === 'desc') ? 'dcricon-arrow-down' : 'dcricon-arrow-up'} ${(!this.settings.stype || this.settings.stype === '' || this.settings.stype === 'startdt') ? '' : 'c-grey-4'} col-sort ms-1"></span></th>` +
      '<th class="va-mid text-center px-2 fs-13i fw-600"><label class="cursor-pointer" data-action="click->financedetail#sortByEndDate">End Date</label>' +
      `<span data-action="click->financedetail#sortByEndDate" class="${(this.settings.stype === 'enddt' && this.settings.order === 'desc') ? 'dcricon-arrow-down' : 'dcricon-arrow-up'} ${this.settings.stype !== 'enddt' ? 'c-grey-4' : ''} col-sort ms-1"></span></th>` +
      '<th class="va-mid text-right px-2 fs-13i fw-600"><label class="cursor-pointer" data-action="click->financedetail#sortByBudget">Budget</label>' +
      `<span data-action="click->financedetail#sortByBudget" class="${(this.settings.stype === 'budget' && this.settings.order === 'desc') ? 'dcricon-arrow-down' : 'dcricon-arrow-up'} ${this.settings.stype !== 'budget' ? 'c-grey-4' : ''} col-sort ms-1"></span></th>` +
      '<th class="va-mid text-right px-2 fs-13i fw-600"><label class="cursor-pointer" data-action="click->financedetail#sortByDays">Days</label>' +
      `<span data-action="click->financedetail#sortByDays" class="${(this.settings.stype === 'days' && this.settings.order === 'desc') ? 'dcricon-arrow-down' : 'dcricon-arrow-up'} ${this.settings.stype !== 'days' ? 'c-grey-4' : ''} col-sort ms-1"></span></th>` +
      '<th class="va-mid text-right px-2 fs-13i fw-600"><label class="cursor-pointer" data-action="click->financedetail#sortByAvg">Monthly Avg (Est)</label>' +
      `<span data-action="click->financedetail#sortByAvg" class="${(this.settings.stype === 'avg' && this.settings.order === 'desc') ? 'dcricon-arrow-down' : 'dcricon-arrow-up'} ${this.settings.stype !== 'avg' ? 'c-grey-4' : ''} col-sort ms-1"></span></th>` +
      '<th class="va-mid text-right px-2 fs-13i fw-600"><label class="cursor-pointer" data-action="click->financedetail#sortBySpent">Total Spent (Est)</label>' +
      `<span data-action="click->financedetail#sortBySpent" class="${(this.settings.stype === 'spent' && this.settings.order === 'desc') ? 'dcricon-arrow-down' : 'dcricon-arrow-up'} ${this.settings.stype !== 'spent' ? 'c-grey-4' : ''} col-sort ms-1"></span></th>` +
      '<th class="va-mid text-right px-2 fs-13i fw-600 pr-10i"><label class="cursor-pointer" data-action="click->financedetail#sortByRemaining">Total Remaining (Est)</label>' +
      `<span data-action="click->financedetail#sortByRemaining" class="${(this.settings.stype === 'remaining' && this.settings.order === 'desc') ? 'dcricon-arrow-down' : 'dcricon-arrow-up'} ${this.settings.stype !== 'remaining' ? 'c-grey-4' : ''} col-sort ms-1"></span></th>` +
      '</tr></thead>'

    let tbody = '<tbody>###</tbody>'
    let bodyList = ''
    let totalBudget = 0
    let totalAllSpent = 0
    let totalRemaining = 0
    // create tbody content
    const summaryList = this.sortSummary(data)
    for (let i = 0; i < summaryList.length; i++) {
      const summary = summaryList[i]
      const lengthInDays = this.getLengthInDay(summary)
      let monthlyAverage = summary.budget / lengthInDays
      if (lengthInDays < 30) {
        monthlyAverage = summary.budget
      } else {
        monthlyAverage = monthlyAverage * 30
      }
      totalBudget += summary.budget
      totalAllSpent += summary.totalSpent
      totalRemaining += summary.totalRemaining > 0 ? summary.totalRemaining : 0
      bodyList += `<tr class="${summary.totalRemaining > 0 ? 'summary-active-row' : 'proposal-summary-row'}">` +
        `<td class="va-mid text-center fs-13i"><a href="${'/finance-report/detail?type=proposal&token=' + summary.token}" class="link-hover-underline fs-13i">${summary.name}</a></td>`
      if (!hideAuthor) {
        bodyList += `<td class="va-mid text-center fs-13i"><a href="${'/finance-report/detail?type=owner&name=' + summary.author}" class="link-hover-underline fs-13i">${summary.author}</a></td>`
      }
      if (!hideDomain) {
        bodyList += `<td class="va-mid text-center fs-13i"><a href="${'/finance-report/detail?type=domain&name=' + summary.domain}" class="link-hover-underline fs-13i">${summary.domain.charAt(0).toUpperCase() + summary.domain.slice(1)}</a></td>`
      }
      bodyList += `<td class="va-mid text-center fs-13i">${summary.start}</td>` +
        `<td class="va-mid text-center fs-13i">${summary.end}</td>` +
        `<td class="va-mid text-right px-2 fs-13i">$${humanize.formatToLocalString(summary.budget, 2, 2)}</td>` +
        `<td class="va-mid text-right fs-13i">${lengthInDays}</td>` +
        `<td class="va-mid text-right px-2 fs-13i">$${humanize.formatToLocalString(monthlyAverage, 2, 2)}</td>` +
        `<td class="va-mid text-right px-2 fs-13i">${summary.totalSpent > 0 ? '$' + humanize.formatToLocalString(summary.totalSpent, 2, 2) : ''}</td>` +
        `<td class="va-mid text-right px-2 fs-13i pr-10i">${summary.totalRemaining > 0 ? '$' + humanize.formatToLocalString(summary.totalRemaining, 2, 2) : '-'}</td>` +
        '</tr>'
    }
    const totalColSpan = hideAuthor && hideDomain ? '3' : ((!hideAuthor && hideDomain) || (hideAuthor && !hideDomain) ? '4' : '5')
    bodyList += '<tr class="text-secondary finance-table-header finance-table-footer last-row-header">' +
    `<td class="va-mid text-center fw-600 fs-13i" colspan="${totalColSpan}">Total</td>` +
    `<td class="va-mid text-right px-2 fw-600 fs-13i">$${humanize.formatToLocalString(totalBudget, 2, 2)}</td>` +
    '<td>-</td><td>-</td>' +
    `<td class="va-mid text-right px-2 fw-600 fs-13i">${totalAllSpent > 0 ? '$' + humanize.formatToLocalString(totalAllSpent, 2, 2) : '-'}</td>` +
    `<td class="va-mid text-right px-2 fw-600 fs-13i">${totalRemaining > 0 ? '$' + humanize.formatToLocalString(totalRemaining, 2, 2) : '-'}</td>` +
    '</tr>'
    tbody = tbody.replace('###', bodyList)
    return thead + tbody
  }

  getLengthInDay (summary) {
    const start = Date.parse(summary.start)
    const end = Date.parse(summary.end)
    const oneDay = 24 * 60 * 60 * 1000

    return Math.round(Math.abs((end - start) / oneDay))
  }

  getYearDataFromMonthData (data) {
    const result = []
    const yearDataMap = new Map()
    const yearDataDcrMap = new Map()
    const yearArr = []
    data.monthData.forEach((item) => {
      const monthArr = item.month.split('-')
      if (monthArr.length !== 2) {
        return
      }
      const year = monthArr[0]
      if (!yearArr.includes(year)) {
        yearArr.push(year)
      }
      if (yearDataMap.has(year)) {
        yearDataMap.set(year, yearDataMap.get(year) + item.expense)
        yearDataDcrMap.set(year, yearDataDcrMap.get(year) + item.expenseDcr)
      } else {
        yearDataMap.set(year, item.expense)
        yearDataDcrMap.set(year, item.expenseDcr)
      }
    })

    yearArr.forEach((year) => {
      const object = {
        month: year,
        expense: yearDataMap.get(year),
        expenseDcr: yearDataDcrMap.get(year)
      }
      result.push(object)
    })
    return result
  }

  getFullTimeParam (timeInput, splitChar) {
    const timeArr = timeInput.split(splitChar)
    let timeParam = ''
    if (timeArr.length === 2) {
      timeParam = timeArr[0] + '_'
      // if month < 10
      if (timeArr[1].charAt(0) === '0') {
        timeParam += timeArr[1].substring(1, timeArr[1].length)
      } else {
        timeParam += timeArr[1]
      }
    }
    return timeParam
  }

  createMonthYearTable (data, type) {
    let handlerData = data.monthData
    if (type === 'year') {
      handlerData = this.getYearDataFromMonthData(data)
    }
    let breakTable = 7
    if (type === 'year' || this.settings.type === 'proposal') {
      // No break
      breakTable = 50
    }
    return this.createTableDetailForMonthYear(handlerData, breakTable, type)
  }

  createTableDetailForMonthYear (handlerData, breakTable, type) {
    let allTable = ''
    let count = 0
    let stepNum = 0
    for (let i = 0; i < handlerData.length; i++) {
      if (count === 0) {
        allTable += `<table class="table monthly v3 border-grey-2 w-auto ${stepNum > 0 ? 'ms-2' : ''}" style="height: 40px;">` +
        '<col><colgroup span="2"></colgroup><thead>' +
        `<tr class="text-secondary finance-table-header"><th rowspan="2" class="va-mid text-center px-2 fs-13i fw-600">${type === 'year' ? 'Year' : 'Month'}</th>` +
        '<th colspan="2" scope="colgroup" class="va-mid text-center-i fs-13i fw-600">Spent (Est)</th>'
        allTable += (this.settings.type === 'year' ? '<th colspan="2" scope="colgroup" class="va-mid text-center-i fs-13i fw-600">Actual Spent</th>' : '') + '</tr>'
        allTable += '<tr class="text-secondary finance-table-header">' +
        '<th scope="col" class="va-mid text-center-i fs-13i fw-600">USD</th>' +
        '<th scope="col" class="va-mid text-center-i fs-13i fw-600">DCR</th>'
        allTable += this.settings.type === 'year' ? '<th scope="col" class="va-mid text-center-i fs-13i fw-600">USD</th><th scope="col" class="va-mid text-center-i fs-13i fw-600">DCR</th>' : ''
        allTable += '</tr></thead><tbody>'
      }
      const dataMonth = handlerData[i]
      let isFuture = false
      const timeYearMonth = this.getYearMonthArray(dataMonth.month, '-')
      const nowDate = new Date()
      const year = nowDate.getUTCFullYear()
      const month = nowDate.getUTCMonth() + 1
      if (type === 'year') {
        isFuture = timeYearMonth[0] > year
      } else if (type === 'month') {
        const compareDataTime = timeYearMonth[0] * 12 + timeYearMonth[1]
        const compareNowTime = year * 12 + month
        isFuture = compareDataTime > compareNowTime
      }
      allTable += `<tr class="odd-even-row ${isFuture ? 'future-row-data' : ''}">`
      const timeParam = this.getFullTimeParam(dataMonth.month, '-')
      allTable += `<td class="text-left px-2 fs-13i"><a class="link-hover-underline fs-13i fw-600" style="text-align: right; width: 80px;" href="${'/finance-report/detail?type=' + type + '&time=' + (timeParam === '' ? dataMonth.month : timeParam)}">${dataMonth.month}</a></td>`
      allTable += `<td class="text-right px-2 fs-13i">${dataMonth.expense !== 0.0 ? '$' + humanize.formatToLocalString(dataMonth.expense, 2, 2) : '-'}</td>` +
                  `<td class="text-right px-2 fs-13i">${dataMonth.expenseDcr !== 0.0 ? humanize.formatToLocalString(dataMonth.expenseDcr / 1e8, 2, 2) : '-'}</td>`
      if (this.settings.type === 'year') {
        allTable += `<td class="text-right px-2 fs-13i">${dataMonth.actualExpense !== 0.0 ? '$' + humanize.formatToLocalString(dataMonth.actualExpense, 2, 2) : '-'}</td>` +
                    `<td class="text-right px-2 fs-13i">${dataMonth.actualExpenseDcr !== 0.0 ? humanize.formatToLocalString(dataMonth.actualExpenseDcr / 1e8, 2, 2) : '-'}</td>`
      }
      allTable += '</tr>'
      if (count === breakTable) {
        allTable += '</tbody>'
        allTable += '</table>'
        count = 0
      } else {
        count++
      }
      stepNum++
    }
    if (count !== breakTable) {
      allTable += '</tbody>'
      allTable += '</table>'
    }
    return allTable
  }

  // Calculate and response
  async yearMonthCalculate () {
    // init report time range
    await this.initReportTimeRange()
    this.handlerYearMonthNextPrevButton()
    // set up navigative to main report and up level of time
    let monthYearDisplay = this.settings.time.toString().replace('_', '-')
    this.toUpReportTarget.href = '/finance-report'
    let reportType
    if (this.settings.type === 'year') {
      this.yearBreadcumbTarget.classList.add('d-none')
      reportType = 'Yearly Summary Report'
    } else {
      reportType = 'Monthly Summary Report'
      this.yearBreadcumbTarget.classList.remove('d-none')
      if (this.settings.time) {
        const timeArr = this.settings.time.trim().split('_')
        if (timeArr.length >= 2) {
          const year = parseInt(timeArr[0])
          this.yearBreadcumbTarget.href = '/finance-report/detail?type=year&time=' + year
          this.yearBreadcumbTarget.textContent = year
        }
      }
      const myArr = this.settings.time.toString().split('_')
      if (myArr.length >= 2) {
        const monthNumber = Number(myArr[1])
        const date = new Date()
        date.setMonth(monthNumber - 1)
        monthYearDisplay = date.toLocaleString('en-US', { month: 'long' }) + ' ' + myArr[0]
      }
    }
    this.currentDetailTarget.textContent = monthYearDisplay
    this.detailReportTitleTarget.textContent = reportType + ' - ' + monthYearDisplay
    const url = `/api/finance-report/detail?type=${this.settings.type}&time=${this.settings.time}`
    let response
    requestCounter++
    const thisRequest = requestCounter
    if (hasCache(url)) {
      response = responseCache[url]
    } else {
      // response = await axios.get(url)
      response = await requestJSON(url)
      responseCache[url] = response
      if (thisRequest !== requestCounter) {
        // new request was issued while waiting.
        console.log('Response request different')
      }
    }

    if (!response) {
      this.proposalAreaTarget.classList.add('d-none')
      return
    }
    responseData = response
    this.proposalReportTarget.innerHTML = this.createProposalDetailReport(response)
    if (response.proposalTotal > 0) {
      this.proposalTopSummaryTarget.classList.remove('d-none')
      this.handlerSummaryArea(response)
      this.createDomainsSummaryTable(response)
    } else {
      this.proposalTopSummaryTarget.classList.add('d-none')
    }
    this.createYearMonthTopSummary(response)
    // create month data list if type is year
    if (this.settings.type === 'year') {
      if (response.monthlyResultData && response.monthlyResultData.length > 0) {
        this.monthlyAreaTarget.classList.remove('d-none')
        this.monthlyReportTarget.innerHTML = this.createTableDetailForMonthYear(response.monthlyResultData, 12, 'month')
      } else {
        this.monthlyAreaTarget.classList.add('d-none')
      }
    }
  }

  handlerSummaryArea (data) {
    this.expendiduteValueTarget.textContent = '$' + humanize.formatToLocalString(data.proposalTotal, 2, 2)
    // display proposal spent value
    if (!data.reportDetail || data.reportDetail.length === 0) {
      return
    }
    let totalSpent = 0
    let totalSpentDCR = 0
    for (let i = 0; i < data.reportDetail.length; i++) {
      const report = data.reportDetail[i]
      totalSpent += report.spentEst > 0 ? report.spentEst : 0
      totalSpentDCR += report.totalSpentDcr > 0 ? report.totalSpentDcr : 0
    }
    if (totalSpent > 0) {
      this.proposalSpentAreaTarget.classList.remove('d-none')
      this.proposalSpentTarget.textContent = '$' + humanize.formatToLocalString(totalSpent, 2, 2) + ` (${humanize.formatToLocalString(totalSpentDCR, 2, 2)} DCR)`
    } else {
      this.proposalSpentAreaTarget.classList.add('d-none')
    }
    // display treasury spent value
    if (totalSpent > 0) {
      this.treasurySpentAreaTarget.classList.remove('d-none')
      this.unaccountedValueAreaTarget.classList.remove('d-none')
      const combinedUSD = data.treasurySummary.outvalueUSD + data.legacySummary.outvalueUSD
      const combinedDCR = data.treasurySummary.outvalue + data.legacySummary.outvalue
      this.treasurySpentTarget.textContent = '$' + humanize.formatToLocalString(combinedUSD, 2, 2) + ` (${humanize.formatToLocalString(combinedDCR / 100000000, 2, 2)} DCR)`
      const deltaUSD = combinedUSD - totalSpent
      const deltaDCR = combinedDCR / 100000000 - totalSpentDCR
      this.unaccountedValueTarget.textContent = (deltaUSD < 0 ? '-' : '') + '$' + humanize.formatToLocalString(Math.abs(deltaUSD), 2, 2) + ` (${humanize.formatToLocalString(deltaDCR, 2, 2)} DCR, ${deltaUSD < 0 ? 'Missing' : 'Unaccounted'})`
    } else {
      this.treasurySpentAreaTarget.classList.add('d-none')
      this.unaccountedValueAreaTarget.classList.add('d-none')
    }
  }

  createDomainDetailReport (data) {
    if (!data.reportDetail || data.reportDetail.length === 0) {
      return ''
    }
    const domainMap = new Map()
    data.reportDetail.forEach((detail) => {
      if (domainMap.has(detail.domain)) {
        domainMap.set(detail.domain, domainMap.get(detail.domain) + detail.expense)
      } else {
        domainMap.set(detail.domain, detail.expense)
      }
    })
    let tbody = '<tbody>###</tbody>'

    let bodyList = ''
    for (let i = 0; i < data.domainList.length; i++) {
      const domain = data.domainList[i]
      bodyList += '<tr>'
      // td domain name
      bodyList += `<td class="text-left fs-13i"><a href="${'/finance-report/detail?type=domain&name=' + domain}" class="link-hover-underline fs-13i">${domain.charAt(0).toUpperCase() + domain.slice(1)}</a></td>`
      bodyList += `<td class="text-right fs-13i">$${humanize.formatToLocalString(domainMap.get(domain), 2, 2)}</td>`
      bodyList += '</tr>'
    }
    tbody = tbody.replace('###', bodyList)
    return tbody
  }

  createYearMonthTopSummary (data) {
    if (data.treasurySummary.invalue <= 0 && data.treasurySummary.outvalue <= 0 && data.legacySummary.invalue <= 0 && data.legacySummary.outvalue <= 0) {
      this.totalSpanRowTarget.classList.add('d-none')
      return
    }
    this.totalSpanRowTarget.classList.remove('d-none')
    let innerHtml = '<col><colgroup span="2"></colgroup>' +
    '<thead><tr class="text-secondary finance-table-header"><th rowspan="2" class="va-mid text-center px-2 fs-13i fw-600">Treasury Type</th>' +
    '<th colspan="2" scope="colgroup" class="va-mid text-center-i fs-13i fw-600">Value</th></tr>' +
    '<tr class="text-secondary finance-table-header"><th scope="col" class="va-mid text-center-i fs-13i fw-600">DCR</th>' +
    '<th scope="col" class="va-mid text-center-i fs-13i fw-600">USD</th></tr></thead><tbody>'
    innerHtml += data.treasurySummary.invalue > 0
      ? `<tr class="odd-even-row"><td class="text-left px-2 fs-13i">Decentralized Income</td><td class="text-right px-2 fs-13i">${humanize.formatToLocalString((data.treasurySummary.invalue / 100000000), 3, 3) + ' DCR'}</td>` +
    `<td class="text-right px-2 fs-13i">$${humanize.formatToLocalString((data.treasurySummary.invalueUSD), 2, 2)}</td></tr>`
      : ''
    innerHtml += data.treasurySummary.outvalue > 0
      ? `<tr class="odd-even-row"><td class="text-left px-2 fs-13i">Decentralized Outgoing</td><td class="text-right px-2 fs-13i">${humanize.formatToLocalString((data.treasurySummary.outvalue / 100000000), 3, 3) + ' DCR'}</td>` +
    `<td class="text-right px-2 fs-13i">$${humanize.formatToLocalString((data.treasurySummary.outvalueUSD), 2, 2)}</td></tr>`
      : ''
    innerHtml += data.legacySummary.invalue > 0
      ? `<tr class="odd-even-row"><td class="text-left px-2 fs-13i">Admin Income</td><td class="text-right px-2 fs-13i">${humanize.formatToLocalString((data.legacySummary.invalue / 100000000), 3, 3) + ' DCR'}</td>` +
    `<td class="text-right px-2 fs-13i">$${humanize.formatToLocalString((data.legacySummary.invalueUSD), 2, 2)}</td></tr>`
      : ''
    innerHtml += data.legacySummary.outvalue > 0
      ? `<tr class="odd-even-row"><td class="text-left px-2 fs-13i">Admin Outgoing</td><td class="text-right px-2 fs-13i">${humanize.formatToLocalString((data.legacySummary.outvalue / 100000000), 3, 3) + ' DCR'}</td>` +
    `<td class="text-right px-2 fs-13i">$${humanize.formatToLocalString((data.legacySummary.outvalueUSD), 2, 2)}</td></tr>`
      : ''
    innerHtml += '</tbody>'
    this.yearMonthInfoTableTarget.innerHTML = innerHtml
  }

  createDomainsSummaryTable (data) {
    const domainDataMap = this.getDomainsSummaryData(data)
    let innerHtml = '<col><colgroup span="2"></colgroup>' +
    '<thead><tr class="text-secondary finance-table-header"><th rowspan="2" class="va-mid text-center px-2 fs-13i fw-600">Domain</th>' +
    '<th colspan="2" scope="colgroup" class="va-mid text-center-i fs-13i fw-600">Spent (Est)</th></tr>' +
    '<tr class="text-secondary finance-table-header"><th scope="col" class="va-mid text-center-i fs-13i fw-600">DCR</th>' +
    '<th scope="col" class="va-mid text-center-i fs-13i fw-600">USD</th></tr></thead><tbody>'
    let totalDCR = 0; let totalUSD = 0
    let hasData = false
    domainDataMap.forEach((val, key) => {
      if (val.valueDCR !== 0 || val.valueUSD !== 0) {
        const valueDCR = val.valueDCR
        totalDCR += val.valueDCR
        const valueUSD = val.valueUSD
        totalUSD += val.valueUSD
        hasData = true
        innerHtml += `<tr class="odd-even-row"><td class="text-left px-2 fs-13i"><a href="/finance-report/detail?type=domain&name=${key}" class="link-hover-underline fs-13i">${key.charAt(0).toUpperCase() + key.slice(1)}</a></td>` +
                     `<td class="text-right px-2 fs-13i">${valueDCR > 0 ? humanize.formatToLocalString(valueDCR, 2, 2) : '-'}</td>` +
                     `<td class="text-right px-2 fs-13i">$${valueUSD > 0 ? humanize.formatToLocalString(valueUSD, 2, 2) : '-'}</td></tr>`
      }
    })
    if (!hasData) {
      this.domainSummaryAreaTarget.classList.add('d-none')
      return
    }
    this.domainSummaryAreaTarget.classList.remove('d-none')
    innerHtml += '<tr class="finance-table-header finance-table-footer last-row-header">' +
      '<td class="va-mid text-center fw-600 fs-13i">Total</td>' +
      `<td class="va-mid text-right px-2 fw-600 fs-13i">${totalDCR > 0 ? humanize.formatToLocalString(totalDCR, 2, 2) : '-'}</td>` +
      `<td class="va-mid text-right px-2 fw-600 fs-13i">${totalUSD > 0 ? '$' + humanize.formatToLocalString(totalUSD, 2, 2) : '-'}</td>` +
      '</tr>'
    innerHtml += '</tbody>'
    this.domainSummaryTableTarget.innerHTML = innerHtml
  }

  sortByPName () {
    this.proposalSort('pname')
  }

  sortByAuthor () {
    this.proposalSort('author')
  }

  sortByDomain () {
    this.proposalSort('domain')
  }

  sortByStartDate () {
    this.proposalSort('startdt')
  }

  sortByEndDate () {
    this.proposalSort('enddt')
  }

  sortBySpent () {
    this.proposalSort('spent')
  }

  sortByBudget () {
    this.proposalSort('budget')
  }

  sortByDays () {
    this.proposalSort('days')
  }

  sortByAvg () {
    this.proposalSort('avg')
  }

  sortByRemaining () {
    this.proposalSort('remaining')
  }

  proposalSort (type) {
    this.settings.stype = type
    this.settings.order = this.settings.order === 'esc' ? 'desc' : 'esc'
    if (this.settings.type === 'year' || this.settings.type === 'month') {
      this.yearMonthProposalListUpdate()
    } else {
      this.proposalDetailsListUpdate()
    }
  }

  createProposalDetailReport (data) {
    if (!data.reportDetail || data.reportDetail.length === 0) {
      this.proposalAreaTarget.classList.add('d-none')
      return ''
    }

    if (!this.settings.stype || this.settings.stype === '') {
      this.settings.stype = 'pname'
    }

    this.proposalAreaTarget.classList.remove('d-none')
    const thead = '<thead>' +
    '<tr class="text-secondary finance-table-header">' +
    '<th class="va-mid text-center px-2 fs-13i fw-600"><label class="cursor-pointer" data-action="click->financedetail#sortByPName">Proposal Name</label>' +
    `<span data-action="click->financedetail#sortByPName" class="${(this.settings.stype === 'pname' && this.settings.order === 'desc') ? 'dcricon-arrow-down' : 'dcricon-arrow-up'} ${this.settings.stype !== 'pname' ? 'c-grey-4' : ''} col-sort ms-1"></span></th>` +
    '<th class="va-mid text-center px-2 fs-13i fw-600"><label class="cursor-pointer" data-action="click->financedetail#sortByDomain">Domain</label>' +
    `<span data-action="click->financedetail#sortByDomain" class="${(this.settings.stype === 'domain' && this.settings.order === 'desc') ? 'dcricon-arrow-down' : 'dcricon-arrow-up'} ${this.settings.stype !== 'domain' ? 'c-grey-4' : ''} col-sort ms-1"></span></th>` +
    `<th class="va-mid text-right px-2 fs-13i fw-600"><label class="cursor-pointer" data-action="click->financedetail#sortBySpent">This ${this.settings.type === 'year' ? 'Year' : 'Month'} (Est)</label>` +
    `<span data-action="click->financedetail#sortBySpent" class="${(this.settings.stype === 'spent' && this.settings.order === 'desc') ? 'dcricon-arrow-down' : 'dcricon-arrow-up'} ${this.settings.stype !== 'spent' ? 'c-grey-4' : ''} col-sort ms-1"></span></th>` +
    '</tr></thead>'

    let tbody = '<tbody>###</tbody>'
    let bodyList = ''
    let totalExpense = 0
    // sort by startdt
    const summaryList = this.sortSummary(data.reportDetail)
    for (let i = 0; i < summaryList.length; i++) {
      bodyList += '<tr class="odd-even-row">'
      const report = summaryList[i]
      // add proposal name
      bodyList += '<td class="va-mid px-2 text-left fs-13i">' +
      `<a href="${'/finance-report/detail?type=proposal&token=' + report.token}" class="link-hover-underline fs-13i d-block">${report.name}</a></td>` +
      `<td class="va-mid text-center px-2 fs-13i"><a href="${'/finance-report/detail?type=domain&name=' + report.domain}" class="link-hover-underline fs-13i">${report.domain.charAt(0).toUpperCase() + report.domain.slice(1)}</a></td>` +
        '<td class="va-mid text-right px-2 fs-13i">' +
        `${report.totalSpent > 0 ? '$' + humanize.formatToLocalString(report.totalSpent, 2, 2) : ''}</td></tr>`
      totalExpense += report.totalSpent
    }

    bodyList += '<tr class="finance-table-header finance-table-footer last-row-header">' +
    '<td class="va-mid text-center fw-600 fs-13i" colspan="2">Total</td>' +
    `<td class="va-mid text-right px-2 fw-600 fs-13i">${totalExpense > 0 ? '$' + humanize.formatToLocalString(totalExpense, 2, 2) : ''}</td>` +
    '</tr>'
    tbody = tbody.replace('###', bodyList)
    return thead + tbody
  }

  getDomainsSummaryData (data) {
    const result = new Map()
    if (!data.reportDetail || data.reportDetail.length === 0) {
      return result
    }
    for (let i = 0; i < data.reportDetail.length; i++) {
      const report = data.reportDetail[i]
      const domain = report.domain
      if (result.has(domain)) {
        const detailData = {}
        const existData = result.get(domain)
        detailData.valueDCR = existData.valueDCR + (report.totalSpentDcr > 0 ? report.totalSpentDcr : 0)
        detailData.valueUSD = existData.valueUSD + (report.spentEst > 0 ? report.spentEst : 0)
        result.set(domain, detailData)
      } else {
        const detailData = {}
        detailData.valueDCR = report.totalSpentDcr > 0 ? report.totalSpentDcr : 0
        detailData.valueUSD = report.spentEst > 0 ? report.spentEst : 0
        result.set(domain, detailData)
      }
    }
    return result
  }

  sortSummary (summary) {
    if (!summary || summary.length === 0) {
      return
    }
    const _this = this
    if (this.settings.stype === 'domain') {
      return this.sortSummaryByDomain(summary)
    }
    summary.sort(function (a, b) {
      let aData = null
      let bData = null
      let alength
      let blength
      switch (_this.settings.stype) {
        case 'pname':
          aData = a.name
          bData = b.name
          break
        case 'author':
          aData = a.author
          bData = b.author
          break
        case 'budget':
          aData = a.budget
          bData = b.budget
          break
        case 'spent':
          aData = a.totalSpent
          bData = b.totalSpent
          break
        case 'remaining':
          aData = a.totalRemaining
          bData = b.totalRemaining
          break
        case 'days':
          aData = _this.getLengthInDay(a)
          bData = _this.getLengthInDay(b)
          break
        case 'avg':
          alength = _this.getLengthInDay(a)
          blength = _this.getLengthInDay(b)
          aData = (a.budget / alength) * 30
          bData = (b.budget / blength) * 30
          break
        case 'enddt':
          aData = Date.parse(a.end)
          bData = Date.parse(b.end)
          break
        default:
          aData = Date.parse(a.start)
          bData = Date.parse(b.start)
          break
      }

      if (aData > bData) {
        return _this.settings.order === 'desc' ? -1 : 1
      }
      if (aData < bData) {
        return _this.settings.order === 'desc' ? 1 : -1
      }
      return 0
    })

    return summary
  }

  sortSummaryByDomain (summary) {
    if (!summary) {
      return
    }
    const _this = this
    summary.sort(function (a, b) {
      if (a.domain > b.domain) {
        return _this.settings.order === 'desc' ? -1 : 1
      } else if (a.domain < b.domain) {
        return _this.settings.order === 'desc' ? 1 : -1
      } else {
        if (a.name > b.name) {
          return _this.settings.order === 'desc' ? -1 : 1
        }
        if (a.name < b.name) {
          return _this.settings.order === 'desc' ? 1 : -1
        }
      }
      return 0
    })

    return summary
  }

  getYearMonthArray (timeInput, splitChar) {
    const timeArr = timeInput.split(splitChar)
    const result = []
    if (timeArr.length < 2) {
      result.push(parseInt(timeArr[0]))
      return result
    }
    result.push(parseInt(timeArr[0]))
    result.push(parseInt(timeArr[1]))
    return result
  }
}
